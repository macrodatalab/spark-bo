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

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, Partition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with Logging {

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /** Returns a new base relation with the given parameters and schema. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))

    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    BORelation(url, table, schema, properties)(sqlContext)
  }

  /** Returns a new base relation with the given parameters and DataFrame. */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
	val urls = url.split(",")
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))
    val overwrite = (mode == SaveMode.Overwrite)
    var exist = false

    var u = null
    for (u <- urls) {
      if (BORDD.isTableExist(u, table)) {
        exist = true
        if(overwrite) {
          val boApi = new BOIface(u, "cmd", "post", Array(s"DROP TABLE $table"))
          if (boApi.httpStatus != 200 || boApi.status != 0) {
            logError(s"Failed to delete existing $table table. (Http status: ${boApi.httpStatus}, BO status: ${boApi.status})")
            return null
          }
        }
      }
    }
    if (exist) {
      if (mode == SaveMode.ErrorIfExists) {
        sys.error(s"Table $table exists.")
      }
    }
    if (!exist || overwrite) {
      val key = parameters.getOrElse("key", "")
      val sb = new StringBuilder(BORDD.schemaString(data))
      if (key.length() > 0)
        sb.append(s", KEY ($key)")
      val schemaStr = sb.toString()
      val boApi = new BOIface(urls(0), "cmd", "post", Array(s"CREATE TABLE $table ($schemaStr)"))
      if (boApi.httpStatus != 200 || boApi.status != 0) {
        logError(s"Failed to create $table table. (Http status: ${boApi.httpStatus}, BO status: ${boApi.status})")
        return null
      }
    }

    val schema = data.schema
    data.foreachPartition {iter =>
	  // TODO: write to different partition (BO server) separately.
      BORDD.writeData(iter, urls(0), table, schema)
    }

    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    BORelation(url, table, schema, properties)(sqlContext)
  }
}

case class BORelation(
    url: String,
    table: String,
    sch: StructType = null,
    properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  override val needConversion: Boolean = false

  private val urls = url.split(",")
  
  private def checkParams() = {
	if (urls.length == 0 || urls(0).length == 0 || table.length == 0)
	  throw new IllegalArgumentException("No BO server is speciffied.")
  }
  checkParams()

  override val schema: StructType = {
    if (sch != null)
      sch
	else
      BORDD.resolveTable(urls(0), table)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(s"buildScan is called.")
    requiredColumns.foreach(c => logInfo(s"required colume: $c"))
    filters.foreach(f => logInfo(s"filter: $f"))
    BORDD.scanTable(
      sqlContext.sparkContext,
      schema,
      properties,
      table,
      requiredColumns,
      filters,
	  BORDD.getPartition(urls, new Array[String](urls.length)))
  }

  // TODO: we should do just "INSERT INTO", not whole table??
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logInfo(s"insert is called. data: $data, overwrite: $overwrite.")
    val exist = BORDD.isTableExist(urls(0), table)
    if (exist && overwrite) {
      val boApi = new BOIface(urls(0), "cmd", "post", Array(s"DROP TABLE $table"))
      if (boApi.httpStatus != 200 || boApi.status != 0) {
        logError(s"Failed to delete existing $table table. (Http status: ${boApi.httpStatus}, BO status: ${boApi.status})")
        return
      }
      // TODO: check status code
    }
    if (!exist || overwrite) {
      // TODO: add key
      val schString = BORDD.schemaString(data)
      val boApi = new BOIface(urls(0), "cmd", "post", Array(s"CREATE TABLE $table ($schString)"))
      if (boApi.httpStatus != 200 || boApi.status != 0) {
        logError(s"Failed to create $table table. (Http status: ${boApi.httpStatus}, BO status: ${boApi.status})")
        return
      }
      // TODO: check status code
    }

    val sch = data.schema
    data.foreachPartition {iter =>
      BORDD.writeData(iter, urls(0), table, sch)
    }
  }
}
