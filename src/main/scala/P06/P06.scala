package P06

import org.apache.spark.sql.functions._

object P06 extends App {

  val path = "/Users/danielfiedosiuk/Desktop/contacts.csv"
  toUpperColumns(path, Seq("id", "name"))

  def toUpperColumns(path: String, colNames: Seq[String])= {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(path)


    val colNamesReady = colNames.filter(data.schema(_).dataType.typeName == "string")

    colNamesReady.foldLeft(data){ (result, colName) =>
      result.withColumn(s"upper_${colName}", upper(col(colName)))
    }.show
  }
}
