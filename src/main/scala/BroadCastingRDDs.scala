import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastingRDDs extends App {
  val conf = new SparkConf().setAppName("RDD Broadcasting Example").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val airData1JsonFile = "G:\\KafkaFinal\\src\\DataGeneration\\FirstAeroplaneJSON.json"
  val airData2JsonFile = "G:\\KafkaFinal\\src\\DataGeneration\\SecondAeroplaneJSON.json"

  val airData1RDD: RDD[String] = sc.textFile(airData1JsonFile)
  val airData2RDD: RDD[String] = sc.textFile(airData2JsonFile)

  val filterAirData1RDD = airData1RDD.filter(_.contains("SingaporeAirlines"))
  val broadCastingFilteredData: Broadcast[Set[String]] = sc.broadcast(filterAirData1RDD.collect().toSet)

  val processAirData2RDD = airData2RDD.filter(x => broadCastingFilteredData.value.contains(x))
  processAirData2RDD.foreach(println)
  Thread.sleep(800000)
  sc.stop()
}
