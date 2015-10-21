package com.bigobject.spark.examples.byDF

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.bigobject.spark._
import com.bigobject.spark.examples.BoOption

object SumApp {
  def main(args: Array[String]) {
    val option = new BoOption(args)
    val url = option.getString("url")
    val isCache = option.getBoolean("cache")
    val isDFAPI = option.getBoolean("use-df-api")
    var appName = "Sum by Spark SQL"
    if (isDFAPI)
      appName = "Sum by DataFrame API"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)

    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val sales = sqlContext.load("com.bigobject.spark", Map("url" -> url, "dbtable" -> "sales_tbl"))
	sales.registerTempTable("sales")
    if (isCache) {
      // cache sales table
      sales.cache()
      // pre-load sales table
      sales.count()
    }

    if (isDFAPI) {
      // by DataFrame API
      val df = sales.groupBy(sales("channel_name")).agg(sum("total_price"))
      df.show()
    }
    else {
      // by sql statement
	  val df = sqlContext.sql("SELECT channel_name, sum(total_price) FROM sales GROUP BY channel_name")
      df.show()
    }
  }
}

object AvgApp {
  def main(args: Array[String]) {
    val option = new BoOption(args)
    val url = option.getString("url")
    val isCache = option.getBoolean("cache")
    val isDFAPI = option.getBoolean("use-df-api")
    var appName = "Avg by Spark SQL"
    if (isDFAPI)
      appName = "Avg by DataFrame API"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)

    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val sales = sqlContext.load("com.bigobject.spark", Map("url" -> url, "dbtable" -> "sales_tbl"))
	sales.registerTempTable("sales")
    if (isCache) {
      // cache sales table
      sales.cache()
      // pre-load sales table
      sales.count()
    }

    if (isDFAPI) {
      // by DataFrame API
      val df = sales.groupBy(sales("channel_name")).agg(avg("total_price"))
      df.show()
    }
    else {
      // by sql statement
      val df = sqlContext.sql("SELECT channel_name, avg(total_price) FROM sales GROUP BY channel_name")
      df.show()
    }
  }
}

object JoinApp {
  def main(args: Array[String]) {
    val option = new BoOption(args)
    val url = option.getString("url")
    val isCache = option.getBoolean("cache")
    val isDFAPI = option.getBoolean("use-df-api")
    var appName = "Join by Spark SQL"
    if (isDFAPI)
      appName = "Join by DataFrame API"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)

    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val sales = sqlContext.load("com.bigobject.spark", Map("url" -> url, "dbtable" -> "sales_tbl"))
    val prod = sqlContext.load("com.bigobject.spark", Map("url" -> url, "dbtable" -> "Product"))
	sales.registerTempTable("sales")
    prod.registerTempTable("Product")
    if (isCache) {
      // cache sales and Product tables
      sales.cache()
      prod.cache()
      // pre-load sales and Product tables
      sales.count()
      prod.count()
    }

    if (isDFAPI) {
      // by DataFrame API
      val df = sales.join(prod, sales("ProductID") === prod("id")).groupBy(prod("id"), prod("name"), sales("channel_name")).agg(sum("total_price").alias("total_sale")).orderBy(desc("total_sale"))
      df.show()
    }
    else {
      // by sql statement
      val df = sqlContext.sql("SELECT Product.id, Product.name, sales.channel_name, sum(total_price) AS total_sale FROM Product, sales WHERE Product.id = sales.ProductID GROUP BY Product.id, Product.name, sales.channel_name ORDER BY total_sale DESC")
      df.show()
    }
  }
}
