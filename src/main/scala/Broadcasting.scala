import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object Broadcasting extends App {
  val spark = SparkSession.builder()
    .appName("Broadcast")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val airData1JsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\FirstAeroplaneJSON.json"
  val airData2JsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\SecondAeroplaneJSON.json"
  val airData1DF: DataFrame = spark.read.json(airData1JsonFilePath)
  val airData2DF: DataFrame = spark.read.json(airData2JsonFilePath)
  val filteredAirData1DF = airData1DF.filter($"airlines_name"==="AirIndia")
  val broadcastAirData1: Broadcast[Array[AirData1]] = spark.sparkContext.broadcast(filteredAirData1DF.as[AirData1].collect())
  val airData2Count = airData2DF.count()
  println("Filtered Flights Are As Follows :")
  broadcastAirData1.value.foreach(println)
  println(s"\n Number of Flights Data Are As Counted : $airData2Count")
  spark.stop()
}
