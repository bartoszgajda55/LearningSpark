import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
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
