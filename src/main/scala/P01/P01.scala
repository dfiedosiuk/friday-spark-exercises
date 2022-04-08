package P01

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.Console.in

object P01 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = spark.range(50).withColumn("key", $"id" % 5).toDF

  input
    .groupBy("key")
    .agg(collect_set("id"))
    .withColumn(
      "only_first_three",
      slice($"collect_set(id)", 1, 3))
    .show(false)

}
