import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFramesJoins extends App {
  val spark = SparkSession.builder()
    .appName("Broadcasting")
    .master("local[*]")
    .getOrCreate()

  val airData1JsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\FirstAeroplaneJSON.json"
  val airData2JsonFilePath = "G:\\KafkaFinal\\src\\DataGeneration\\SecondAeroplaneJSON.json"

  val firstDF:DataFrame=spark.read.json(airData1JsonFilePath)
  val secondDF:DataFrame=spark.read.json(airData2JsonFilePath)
  val innerJoin=firstDF.join(secondDF,"aircraft_id")
  println("Inner Join Result")
  innerJoin.show()
  val leftJoin=firstDF.join(secondDF,Seq("aircraft_id"),"left")
  println("Left Join Result")
  leftJoin.show()

  println("Right Join Result")
  firstDF.join(secondDF,Seq("aircraft_id"),"Right").show()

  val firstDFNotNull=firstDF.na.fill("Unknown Values",Seq("airlines_name"))
  val secondDFNotNull=secondDF.na.fill(0.0,Seq("aircraft_rating"))

  println("Inner Join After Handling Null Values")
  firstDFNotNull.join(secondDFNotNull,"aircraft_id").show()

  println("Left Join After Handling Null Values")
  firstDFNotNull.join(secondDFNotNull,Seq("aircraft_id"),"left").show()

  println("Right Join After Handling Null Values")
  firstDFNotNull.join(secondDFNotNull,Seq("aircraft_id"),"Right").show()
  Thread.sleep(800000)
  spark.stop()
}