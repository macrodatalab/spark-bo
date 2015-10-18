package com.bigobject.spark.examples.byBO

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.bigobject.spark._
import com.bigobject.spark.examples.BoOption

object SumApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sum by BO")
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
    val option = new BoOption(args)
    val url = option.getString("url")
    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val df = BORDD.sql(sc, url, "SELECT channel_name, sum(total_price) FROM sales GROUP BY channel_name")
    df.show()
  }
}

object AvgApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Avg by BO")
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
    val option = new BoOption(args)
    val url = option.getString("url")
    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val df = BORDD.sql(sc, url, "SELECT channel_name, avg(total_price) FROM sales GROUP BY channel_name")
    df.show()
  }
}

object JoinApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join by BO")
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
    val option = new BoOption(args)
    val url = option.getString("url")
    if (url.length == 0) {
      println("Missing BO url.")
      sys.exit(1)
    }

    val df = BORDD.sql(sc, url, "SELECT Product.id, Product.name, channel_name, sum(total_price) AS total_sale FROM sales GROUP BY Product.id, Product.name, channel_name")// ORDER BY total_sale DESC")
    df.show()
  }
}