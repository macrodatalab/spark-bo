# Spark BO Library

A library for reading and writing [Bigobject](http://www.bigobject.io/) data from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).


## Requirements

This library requires Spark 1.4+

## How to build
The library is built by maven:

```
$ mvn clean package
```

## Using with Spark shell
This package can be added to Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-bo-0.1.jar
```

## What is BO
"BigObject Analytics is an analytic database. It is designed to help users easily gain actionable insights from their data and build data-driven applications."

"BigObject Analytics delivers an analytic framework that unmask the intelligence out of your data."

## Examples
BO data can be queried in pure SQL by registering the data as a (temporary) table.

```sql
CREATE TEMPORARY TABLE Product
USING com.bigobject.spark
OPTIONS (url 'http://127.0.0.1:9090', dbtable 'Product')
```

You can also load BO table by Spark SQL API:

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val product = sqlContext.load("com.bigobject.spark", Map("url" -> "http://127.0.0.1:9090", "dbtable" -> "Product"))
```

One of the BO strength is [Data Analytics](https://docs.bigobject.io/Data_Analytics/index.html). Following examples will show you how to take the advantage of BO Data Analytics engine.

```scala
import com.bigobject.spark._

val url = "http://127.0.0.1:9090"
// build association between products from sales table
BORDD.command(sc, url, "BUILD ASSOCIATION prod2prod (Product.name) BY Customer.id FROM sales")

// get top 10 products that are associated with 'Tropical Berry'
val dfTop10 = BORDD.sql(sc, url, "GET TOP 10 FREQ ('Tropical Berry') FROM prod2prod")

// find (at most) 10 brands whose 40% sales records comes from the first 10% state sales
val sqlFind4010 = "FIND pareto(40,10) Product.brand IN Customer.state BY sum(qty) FROM sales"
val df4010 = BORDD.sql(sc, url, sqlFind4010)
```
