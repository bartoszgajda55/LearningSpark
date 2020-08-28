package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AuthorsAges {
  val spark = SparkSession
    .builder()
    .appName("AuthorsAges")
    .master("local[1]")
    .getOrCreate()

  val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
    ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

  val avgDF = dataDF.groupBy("name").agg(avg("age"))

  avgDF.show()

  def main(args: Array[String]): Unit = {

  }
}
