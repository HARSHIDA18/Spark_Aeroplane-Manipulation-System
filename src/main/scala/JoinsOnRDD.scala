import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object JoinsOnRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AircraftDetails")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val firstJsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\FirstAeroplaneJSON.json"
    val firstDf: DataFrame = spark.read.json(firstJsonFilePath)
    val firstRdd: RDD[AirData1] = firstDf.as[AirData1].rdd

    val secondJsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\SecondAeroplaneJSON.json"
    val secondDf: DataFrame = spark.read.json(secondJsonFilePath)
    val secondRdd: RDD[AirData2] = secondDf.as[AirData2].rdd

    // Inner Join
    val innerJoinedRdd: RDD[(String, (AirData1, AirData2))] = firstRdd.map(a1 => (a1.aircraft_id, a1))
      .join(secondRdd.map(a2 => (a2.aircraft_id, a2)))
    println("Data after inner join")
    innerJoinedRdd.foreach(println)

    // Left Outer Join
    val leftOuterJoinedRdd: RDD[(String, (AirData1, Option[AirData2]))] = firstRdd.map(a1 => (a1.aircraft_id, a1))
      .leftOuterJoin(secondRdd.map(a2 => (a2.aircraft_id, a2)))
    println("Data after left join")
    leftOuterJoinedRdd.foreach(println)

    Thread.sleep(80000)

    spark.stop()
  }
}
