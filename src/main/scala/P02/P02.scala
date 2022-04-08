package P02

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}

object P02 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val words = Seq(Array("hello", "world")).toDF("words").toDF

  val toStringUDF = udf((r: Seq[String]) => r.mkString(" "))

  words.withColumn(
    "solution",
    toStringUDF($"words")
  ).show
}
