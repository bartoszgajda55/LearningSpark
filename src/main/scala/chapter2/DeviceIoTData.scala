package chapter2

import org.apache.spark.sql.SparkSession

object DeviceIoTData {

  val spark = SparkSession.builder()
    .appName("Device IoT Data")
    .master("local[1]")
    .getOrCreate()

  case class DeviceIoTData(battery_level: Long, c02_level: Long,
                           cca2: String, cca3: String, cn: String, device_id: Long,
                           device_name: String, humidity: Long, ip: String, latitude: Double,
                           lcd: String, longitude: Double, scale: String, temp: Long,
                           timestamp: Long)

  import spark.implicits._

  val ds = spark.read
    .json("data/iot-devices/iot_devices.json")
    .as[DeviceIoTData]

  ds.show()

  def main(args: Array[String]): Unit = {}
}
