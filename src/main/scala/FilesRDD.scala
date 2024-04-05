import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
case class Travel(destination_city: String, source_city: String)
case class Price(INR: Double, USD: Double)
case class Time(departure_time: String, arrival_time: String)
case class AirData1(aircraft_id: String, airlines_name: String, travel: Travel, aircraftEmergency_phone: String, time: Time)
case class AirData2(aircraft_id: String, aircraft_rating: Double, price: Price)

object FlightRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AircraftDetails")
      .master("local[*]")
      .getOrCreate()

    val firstJsonFilePath = "C:\\SparkGITHUB\\src\\FakeDataGeneration\\FirstAeroplaneJSON.json"
    val firstDf: DataFrame = spark.read.json(firstJsonFilePath)
    import spark.implicits._
    val firstRdd: RDD[AirData1] = firstDf.as[AirData1].rdd
    firstRdd.foreach(println)

    val secondJsonFilePath = "C:\\SparkGITHUB\\src\\FakeDataGeneration\\SecondAeroplaneJSON.json"
    val secondDf: DataFrame = spark.read.json(secondJsonFilePath)
    val secondRdd: RDD[AirData2] = secondDf.as[AirData2].rdd
    secondRdd.foreach(println)

    val filterAirData1RDD: RDD[AirData1] = firstRdd.filter(x => x.airlines_name == "AirIndia")
    println("The data according to filteration on RDD1 is")
    filterAirData1RDD.foreach(println)

    val filterAirData2RDD: RDD[AirData2] = secondRdd.filter(x => x.aircraft_rating.toDouble > 4.0)
    filterAirData2RDD.foreach(println)

    firstDf.show()
    secondDf.show()

    // Increase sleep time to 10 seconds (10000 milliseconds)
    Thread.sleep(80000)

    spark.stop()
  }
}

