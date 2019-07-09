import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark First Example")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.json("resources/people.json")
    df.show()

    import spark.implicits._
    df.printSchema()
    df.select("name")
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
  }
}
