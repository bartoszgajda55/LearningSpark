package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FireDept {
  val spark = SparkSession
    .builder()
    .appName("Fire Dept Data Job")
    .master("local[1]")
    .getOrCreate()

  val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
  StructField("UnitID", StringType, true),
  StructField("IncidentNumber", IntegerType, true),
  StructField("CallType", StringType, true),
  StructField("CallDate", StringType, true),
  StructField("WatchDate", StringType, true),
  StructField("CallFinalDisposition", StringType, true),
  StructField("AvailableDtTm", StringType, true),
  StructField("Address", StringType, true),
  StructField("City", StringType, true),
  StructField("Zipcode", IntegerType, true),
  StructField("Battalion", StringType, true),
  StructField("StationArea", StringType, true),
  StructField("Box", StringType, true),
  StructField("OriginalPriority", StringType, true),
  StructField("Priority", StringType, true),
  StructField("FinalPriority", IntegerType, true),
  StructField("ALSUnit", BooleanType, true),
  StructField("CallTypeGroup", StringType, true),
  StructField("NumAlarms", IntegerType, true),
  StructField("UnitType", StringType, true),
  StructField("UnitSequenceInCallDispatch", IntegerType, true),
  StructField("FirePreventionDistrict", StringType, true),
  StructField("SupervisorDistrict", StringType, true),
  StructField("Neighborhood", StringType, true),
  StructField("Location", StringType, true),
  StructField("RowID", StringType, true),
  StructField("Delay", FloatType, true)))

  // Read the file using the CSV DataFrameReader
  val sfFireFile="/data/sf-fire/sf-fire-calls.csv"
  val fireDF = spark.read.schema(fireSchema)
    .option("header", "true")
    .csv(sfFireFile)

  val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")

  val fireTsDF = newFireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
      "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")

  // Select the converted columns
  fireTsDF
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, false)

  import spark.implicits._
  fireTsDF
    .select(year($"IncidentDate"))
    .distinct()
    .orderBy(year($"IncidentDate"))
    .show()

  def main(args: Array[String]): Unit = {

  }
}
