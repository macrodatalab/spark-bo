/*
 * Copyright 2015 bigobject.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.bigobject.spark

import java.sql.{SQLException, Timestamp}
import java.util.Properties
import java.io.ByteArrayOutputStream

import scala.util.control._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.convert.Wrappers.{JListWrapper, JMapWrapper}

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{Row, SpecificMutableRow}
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources._

import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.{HttpGet, HttpPost, CloseableHttpResponse}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

import com.fasterxml.jackson.core.{JsonFactory,JsonGenerator,JsonParser,JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper


case class BOPartition(url: String, idx: Int, preSQL: String = null) extends Partition {
  override def index: Int = idx
}

/* BOIface class
 * . wrappers BO RESTful API.
*/
class BOIface(url: String, cmd: String, method: String, stmts: Array[String], async: Boolean = false) extends Logging {

  private val jFactory = new JsonFactory()
  private val mapper = new ObjectMapper()
  
  var error = new ArrayBuffer[String]
  var content = new ArrayBuffer[Map[String, Any]]
  var httpStatus = 0
  val status = exec()
  
  private def convert(obj: Any): Any = obj match {
    case map: java.util.Map[_, _] =>
      JMapWrapper(map).mapValues(convert).map(identity)
    case list: java.util.List[_] =>
      JListWrapper(list).map(convert)
    case atom => atom
  }

  private def exec() : Int = {
    logInfo(s"exec(url: $url, cmd: $cmd)")
    val client = new DefaultHttpClient
    var httpRsp = null.asInstanceOf[CloseableHttpResponse]

    if (method.equalsIgnoreCase("post")) {
      val post = new HttpPost(s"$url/$cmd")

      val qStream = new ByteArrayOutputStream(1024)
      val jG = jFactory.createJsonGenerator(qStream)
      val loop = new Breaks
      loop.breakable {
        for (sql <- stmts) {
          if (sql.length() == 0)
            loop.break
          jG.writeStartObject()
          jG.writeStringField("Stmt", sql)
          if (async) {
            jG.writeObjectFieldStart("Opts")
            jG.writeBooleanField("Handle", true)
            jG.writeEndObject()
          }
          jG.writeEndObject()
        }
      }
      jG.close()

      val qStr = qStream.toString()
      var len = qStr.length
      if (len > 512) len = 512
      logInfo("sql: " + qStr.substring(0, len))
      val se = new StringEntity(qStr, "utf-8")
      post.setEntity(se)
      httpRsp = client.execute(post)
    }
    else
    {
      val get = new HttpGet(s"$url/$cmd")
      httpRsp = client.execute(get)
    }

    httpStatus = httpRsp.getStatusLine().getStatusCode()
    val rspStr = EntityUtils.toString(httpRsp.getEntity(), "utf-8")
    client.close()

    var boStatus = 0
    var offset = 0
    var slen = 0
    val jP = jFactory.createParser(rspStr)
    while (jP.nextToken() != null) {
      jP.skipChildren()
      slen = jP.getCurrentLocation().getCharOffset().asInstanceOf[Int]

      val data = convert(mapper.readValue(rspStr.substring(offset, slen), classOf[Object])).asInstanceOf[Map[String, Any]]
      if (data.contains("Content"))
        content += data("Content").asInstanceOf[Map[String, Any]]
      if (data.contains("Err"))
        error += data("Err").asInstanceOf[String]
      if (data.contains("Status"))
        if (boStatus == 0)
          boStatus = data("Status").asInstanceOf[Int]
      offset = slen + 1
    }
    return boStatus
  }
}

/* BORDD singleton
 * . implements some utilities for BORDD class.
 * . implements BO specific syntax, e.g., FIND.
*/
object BORDD extends Logging {

  val bo2catalyst = Map("STRING" -> StringType,
                        "BYTE" -> ByteType,
                        "INT8" -> ByteType,
                        "INT16" -> ShortType,
                        "INT32" -> IntegerType,
                        "INT64" -> LongType,
                        "FLOAT" -> FloatType,
                        "DOUBLE" -> DoubleType,
                        "DATE32" -> DateType,
                        "DATETIME32" -> TimestampType,
                        "DATETIME64" -> TimestampType)

  val catalyst2bo: Map[DataType, String] = Map(
                        StringType -> "STRING",
                        ByteType -> "BYTE",
                        ShortType -> "INT16",
                        IntegerType -> "INT32",
                        LongType -> "INT64",
                        FloatType -> "FLOAT",
                        DoubleType -> "DOUBLE",
                        BooleanType -> "INT8",
                        DateType -> "DATE32",
                        TimestampType -> "DATETIME64")
  
  def getPartition(urls: Array[String], sqlStr: Array[String]) : Array[Partition] = {
    if (urls.length != sqlStr.length) {
      logError(s"Number of url (${urls.length}) is different from number of SQL statement (${sqlStr.length}).")
      return null.asInstanceOf[Array[Partition]]
    }
    var parts = new ArrayBuffer[Partition]()
    var i = 0
	var url = null
    for (url <- urls) {
      if (url.length > 0)
      {
        parts += BOPartition(url, i, sqlStr(i))
        i += 1
      }
    }
    parts.toArray
  }
  
  def isTableExist(url : String, tblName : String) : Boolean = {
    val boApi = new BOIface(url, "cmd", "post", Array(s"DESC $tblName"))
    if (boApi.httpStatus == 200 && boApi.status == 0)
      true
    else
      false
  }

  def command(
    sc: SparkContext,
    url: String,
    sqlString: String,
    sqlCtx: SQLContext = null) : Int = {
    var sqlSc = sqlCtx
    if (sqlCtx == null)
      sqlSc = new SQLContext(sc)

    val urls = url.split(",")
    urls.foreach{u =>
      val boApi = new BOIface(u, "cmd", "post", Array(sqlString))
      if (boApi.httpStatus != 200 || boApi.status != 0) {
        logError(s"Run command failed. (Http status code: ${boApi.httpStatus}, BO status code: ${boApi.status})")
        return boApi.status
      }
    }
    0
  }

  def sql(
    sc: SparkContext,
    url: String,
    sqlString: String,
    sqlCtx: SQLContext = null) : DataFrame = {
    var sqlSc = sqlCtx
    if (sqlCtx == null)
      sqlSc = new SQLContext(sc)

    val urls = url.split(",")
    val boApi = new BOIface(urls(0), "cmd", "post", Array(sqlString), true)
    if (boApi.httpStatus != 200 || boApi.status != 0) {
      logError(s"Failed to resolve sql statement. (Http status code: ${boApi.httpStatus}, BO status code: ${boApi.status})")
      return null.asInstanceOf[DataFrame]
    }
    if (!boApi.content(0).contains("res")) {
      logError("Invalid BO output: no BO handle.")
      return null.asInstanceOf[DataFrame]
    }
    val handle = boApi.content(0)("res").asInstanceOf[String]
    if (handle.length == 0) {
      logError("Invalid BO output: BO handle is empty.")
      return null.asInstanceOf[DataFrame]
    }
	val schema = resolveSchema(urls(0), s"HDESC $handle")
	if (schema == null)
      return null.asInstanceOf[DataFrame]

    var sqls = new Array[String](urls.length)
    sqls(0) = s"SCAN $handle"
    for (i <- 1 to sqls.length - 1)
      sqls(i) = sqlString

    var allRdd = null.asInstanceOf[RDD[Row]]
    val partitions = getPartition(urls, sqls)
    for (part <- partitions) {
      val boPart = part.asInstanceOf[BOPartition]
      val boRdd = new BORDD(
          sc,
          schema,
          null,
          Array[String](null),
          Array[Filter](null),
          Array[Partition](BOPartition(boPart.url, 0, boPart.preSQL)),
          null)
      if (allRdd == null)
        allRdd = boRdd
      else
        allRdd = allRdd.union(boRdd)
    }
    sqlSc.createDataFrame(allRdd, schema)
  }
  
  def writeData(
      iter: Iterator[Row],
      url: String,
      table: String,
      schema: StructType) = {
    val sb = new StringBuilder(s"INSERT INTO $table VALUES ")
    var rcount = 0
    while (iter.hasNext) {
      val row = iter.next()
      sb.append(insertStmt(row, schema)).append(",")
      rcount += 1
    }
    if (rcount > 0) {
      val boApi = new BOIface(url, "cmd", "post", Array(sb.take(sb.length - 1).toString()))
      if (boApi.httpStatus != 200 || boApi.status != 0)
        logError(s"Failed to insert data into $table table. (Http status code: ${boApi.httpStatus}, BO status code: ${boApi.status})")
      else
        logInfo(s"insert $rcount rows into $table.")
    }
  }

  private def getCatalystType(colType: String): DataType = {
    if (!bo2catalyst.contains(colType))
      throw new SQLException("Unsupported type: " + colType)
    bo2catalyst(colType)
  }

  private def getBOType(colType: DataType): String = {
    if (!catalyst2bo.contains(colType))
      throw new SQLException("Unsupported type: " + colType)
    catalyst2bo(colType)
  }
  
  def insertStmt(row: Row, schema: StructType): String = {
    val sb = new StringBuilder("(")
    val numFields = schema.fields.length
    var i = 0
    while (i < numFields) {
      if (row.isNullAt(i)) {
        sb.append("NULL")
      }
      else {
        schema.fields(i).dataType match {
          case StringType => sb.append("'").append(StringUtils.replace(row.getString(i), "'", "\\'")).append("'")
          case TimestampType => sb.append("'").append(row.getAs[java.sql.Timestamp](i)).append("'")
          case DateType => sb.append("'").append(row.getAs[java.sql.Date](i)).append("'")
          case IntegerType => sb.append(row.getInt(i))
          case LongType => sb.append(row.getLong(i))
          case DoubleType => sb.append(row.getDouble(i))
          case FloatType => sb.append(row.getFloat(i))
          case ShortType => sb.append(row.getShort(i))
          case ByteType => sb.append(row.getByte(i))
          case BooleanType => sb.append(row.getBoolean(i))
          case _ => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $i")
        }
      }
      i += 1
      if (i < numFields)
        sb.append(",")
    }
    sb.append(")").toString()
  }
  
  def makeCsv(iterator: Iterator[Row], schema: StructType): String = {
    val sb = new StringBuilder()
    while (iterator.hasNext) {
      val row = iterator.next()
      val numFields = schema.fields.length
      var i = 0
      while (i < numFields) {
        if (!row.isNullAt(i)) {
          schema.fields(i).dataType match {
            case StringType => sb.append(StringUtils.replace(row.getString(i), "'", "\\'"))
            case TimestampType => sb.append(row.getAs[java.sql.Timestamp](i))
            case DateType => sb.append(row.getAs[java.sql.Date](i))
            case IntegerType => sb.append(row.getInt(i))
            case LongType => sb.append(row.getLong(i))
            case DoubleType => sb.append(row.getDouble(i))
            case FloatType => sb.append(row.getFloat(i))
            case ShortType => sb.append(row.getShort(i))
            case ByteType => sb.append(row.getByte(i))
            case BooleanType => sb.append(row.getBoolean(i))
            case _ => throw new IllegalArgumentException(
                      s"Can't translate non-null value for field $i")
          }
        }
        i += 1
        if (i < numFields)
          sb.append(",")
      }
      sb.append("\n")
    }
    sb.take(sb.length - 1).toString()
  }
  
  def schemaString(df: DataFrame): String = {
    val sb = new StringBuilder()
    val keys = new StringBuilder()
    df.schema.fields foreach { field => {
      val colName = field.name
      val colType = getBOType(field.dataType)
      // All BO data are not NULL
      // val nullable = if (field.nullable) "" else "NOT NULL"
      val nullable = ""
      sb.append(s", $colName $colType $nullable")
      if (field.metadata.contains("key") && field.metadata.getBoolean("key"))
        keys.append(s", $colName")
    }}
    if (keys.length >= 2) {
      val keyStr = keys.substring(2)
      sb.append(s", KEY($keyStr)")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  private def resolveSchema(url: String, schemaCmd: String): StructType = {
    val boApi = new BOIface(url, "cmd", "post", Array(schemaCmd))
    if (boApi.httpStatus != 200 || boApi.status != 0) {
      logError(s"Failed to resolve schema. (Http status code: ${boApi.httpStatus}, BO status code: ${boApi.status})")
      return null.asInstanceOf[StructType]
    }
    if (!boApi.content(0).contains("schema")) {
      logError("Invalid BO output: no BO table schema.")
      return null.asInstanceOf[StructType]
    }
    val schMap = boApi.content(0)("schema").asInstanceOf[Map[String, Any]]
    if (!schMap.contains("attr")) {
      logError("Invalid BO output: no BO table schema attribute.")
      return null.asInstanceOf[StructType]
    }

    var keys = null.asInstanceOf[ArrayBuffer[String]]
    if (schMap.contains("key"))
      keys = schMap("key").asInstanceOf[ArrayBuffer[String]]

    val columns = schMap("attr").asInstanceOf[ArrayBuffer[Map[String, String]]]
	val fields = new Array[StructField](columns.size)
	var i = 0
	for (i <- 0 until columns.size) {
      val col = columns(i)
      val colName = col("name")
	  val metadata = new MetadataBuilder().putString("name", colName)
	  if (keys != null && keys.contains(colName))
	    metadata.putBoolean("key", true)
      fields(i) = StructField(colName, getCatalystType(col("type")), false, metadata.build());
    }
	return new StructType(fields)
  }

  def resolveTable(url: String, table: String): StructType = {
    return resolveSchema(url, s"DESC $table")
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }

  def scanTable(
      sc: SparkContext,
      schema: StructType,
      properties: Properties,
      fqTable: String,
      requiredColumns: Array[String],
      filters: Array[Filter],
      parts: Array[Partition]): RDD[Row] = {
    val sche = pruneSchema(schema, requiredColumns)
    logInfo(s"scanTable is called. pruned schema: $sche.")
    new BORDD(
      sc,
      sche,
      fqTable,
      requiredColumns,
      filters,
      parts,
      properties)
  }

}

/* BORDD class
 * . implements RDD[Row]
*/
class BORDD(
    sc: SparkContext,
    schema: StructType,
    fqTable: String,
    columns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    properties: Properties)
  extends RDD[Row](sc, Nil)
  with Logging {

  override def getPartitions: Array[Partition] = partitions

  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.length == 0) "*" else sb.substring(1)
  }

  private def compileValue(value: Any): Any = value match {
    case stringValue: UTF8String => s"'${escapeSql(stringValue.toString)}'"
    case stringValue: String => s"'${escapeSql(stringValue.toString)}'"
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "\\'")

  private def compileFilter(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case StringStartsWith(attr, value) => s"$attr LIKE ${compileValue(value + '%')}"
    case StringEndsWith(attr, value) => s"$attr LIKE ${compileValue('%' + value)}"
    case StringContains(attr, value) => s"$attr LIKE ${compileValue('%' + value + '%')}"
    case In(attr, value) =>
      val sb = new StringBuilder("")
      value.foreach(x => sb.append(s"${compileValue(x)},"))
      s"$attr IN (${sb.substring(0, sb.length - 1)})"
    case Not(child) => s"NOT (${compileFilter(child)})"
    case And(left, right) => s"${compileFilter(left)} AND ${compileFilter(right)}"
    case Or(left, right) => s"${compileFilter(left)} OR ${compileFilter(right)}"
    case IsNull(attr) => s"IS NULL"
    case _ => null // BO types are not NULL, don't push down "NOT NULL"
  }

  private val filterWhereClause: String = {
    val filterStrings = filters map compileFilter filter (_ != null)
    if (filterStrings.size > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  private def getSqlString(): String = {
    var sqlText = s"SELECT $columnList FROM $fqTable $filterWhereClause"
    val fetchSize = properties.getProperty("fetchSize", "0").toInt
    if (fetchSize > 0) {
      sqlText += s" LIMIT $fetchSize"
    }
    sqlText
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row]
  {
    var finished = false
    var gotNext = false
    var nextValue: Row = null

	// TODO: anything to clean up?
    // context.addTaskCompletionListener{context => onTaskComplete()}
    val part = thePart.asInstanceOf[BOPartition]

    val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))
    var iter = null.asInstanceOf[Iterator[ArrayBuffer[Any]]]
    var sqlStr = part.preSQL
    if (sqlStr == null)
      sqlStr = getSqlString()

    val boApi = new BOIface(part.url, "cmd", "post", Array(sqlStr))
    if (boApi.httpStatus != 200 || boApi.status != 0) {
      logError(s"Failed to get $fqTable table. (Http status code: ${boApi.httpStatus}, BO status code: ${boApi.status})")
      finished = true
    }
    else {
      var rows = new ArrayBuffer[ArrayBuffer[Any]]
      var content = null.asInstanceOf[Map[String, ArrayBuffer[Any]]]
      for (content <- boApi.content
            if content.contains("content")) {
        rows ++= content("content").asInstanceOf[ArrayBuffer[ArrayBuffer[Any]]]
      }
      if (rows.isEmpty) {
        finished = true
      }
      else {
        iter = rows.toIterator
      }
    }

    def getNext(): Row = {
      if (!iter.hasNext) {
          finished = true
          null.asInstanceOf[Row]
      }
      else {
        val row = iter.next()
        var i = 0
        val fields = schema.fields
        for (i <- 0 until fields.size) {
          fields(i).dataType match {
            case DateType =>
              val dateVal = row(i).asInstanceOf[String]
              if (dateVal != null) {
                mutableRow.update(i, DateUtils.millisToDays(DateUtils.stringToTime(dateVal).getTime()))
              } else {
                mutableRow.update(i, null)
              }
            case TimestampType =>
              val dateVal = row(i).asInstanceOf[String]
              if (dateVal != null) {
                mutableRow.update(i, Timestamp.valueOf(dateVal))
              } else {
                mutableRow.update(i, null)
              }
            case DoubleType =>
              val value = row(i)
              value match {
                case value: java.lang.Integer => mutableRow.setDouble(i, value.asInstanceOf[Int].toDouble)
                case _ => mutableRow.setDouble(i, value.asInstanceOf[Double])
              }
            case FloatType =>
              val value = row(i)
              value match {
                case value: java.lang.Integer => mutableRow.setFloat(i, value.asInstanceOf[Int].toFloat)
                case value: java.lang.Double => mutableRow.setFloat(i, value.asInstanceOf[Double].toFloat)
                case _ => mutableRow.setFloat(i, value.asInstanceOf[Float])
              }
            case ByteType =>
              val value = row(i)
              value match {
                case value: java.lang.Integer => mutableRow.setByte(i, value.asInstanceOf[Int].toByte)
                case _ => mutableRow.setByte(i, value.asInstanceOf[Byte])
              }
            case ShortType =>
              val value = row(i)
              value match {
                case value: java.lang.Integer => mutableRow.setShort(i, value.asInstanceOf[Int].toShort)
                case _ => mutableRow.setShort(i, value.asInstanceOf[Short])
              }
            case IntegerType => mutableRow.setInt(i, row(i).asInstanceOf[Int])
            case LongType =>
              val value = row(i)
              value match {
                case value: java.lang.Integer => mutableRow.setLong(i, value.asInstanceOf[Int].toLong)
                case _ => mutableRow.setLong(i, value.asInstanceOf[Long])
              }
            // TODO: use getBytes for better performance, if the encoding is UTF-8
            case StringType => mutableRow.setString(i, row(i).asInstanceOf[String])
          }
        }
        mutableRow
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          gotNext = true
        }
      }
      !finished
    }

    override def next(): Row = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }
}