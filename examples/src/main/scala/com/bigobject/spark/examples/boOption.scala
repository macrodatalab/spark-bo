package com.bigobject.spark.examples

class BoOption(args : Array[String]) {
  private val options = nextOption(Map(), args.toList)

  private def nextOption(opt : Map[String, Any], list: List[String]) : Map[String, Any] = {
    list match {
      case Nil => opt
      case opt1 :: opt2 :: tail if (opt1.startsWith("--") && opt2.startsWith("--")) =>
        nextOption(opt ++ Map(opt1.substring(2) -> true), opt2 :: tail)
      case opt1 :: opt2 :: tail if (opt1.startsWith("--")) =>
        nextOption(opt ++ Map(opt1.substring(2) -> opt2), tail)
      case opt1 :: Nil  if (opt1.startsWith("--")) =>
        nextOption(opt ++ Map(opt1.substring(2) -> true), list.tail)
    }
  }

  def getBoolean(key: String): Boolean = {
    if (options.contains(key))
      options(key).asInstanceOf[Boolean]
    else
      false
  }

  def getString(key: String): String = {
    if (options.contains(key))
      options(key).asInstanceOf[String]
    else
      ""
  }

  def getInt(key: String): Int = {
    if (options.contains(key))
      options(key).asInstanceOf[String].toInt
    else
      0
  }
}