package playground

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TaxiETL {
  def main(args: Array[String]): Unit = {

//    if(args.length != 2) { // prod version only
//      println("Need two arguments: 1) input data path 2) output data path")
//    }

    val spark = SparkSession.builder()
      .appName("Taxi ETL")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[1]") // dev version only
      .getOrCreate()

    val devDataPath: String = "data/taxi/raw/green_tripdata_2019-01.csv"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(devDataPath) // dev
//      .csv(args(0)) // prod

    val dfWithDateColumns = df
      .withColumn("year", year(col("lpep_pickup_datetime")))
      .withColumn("month", month(col("lpep_pickup_datetime")))
      .withColumn("day", dayofmonth(col("lpep_pickup_datetime")))

    dfWithDateColumns.show()

    dfWithDateColumns
      .repartition(col("year"), col("month"), col("day"))
      .write
      .partitionBy("year", "month", "day")
      .mode(SaveMode.Overwrite)
      .csv("data/taxi/transformed") // dev
//      .csv(args(1)) // prod
  }
}
