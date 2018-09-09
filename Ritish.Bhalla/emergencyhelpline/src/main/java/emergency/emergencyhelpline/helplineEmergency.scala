package emergency.emergencyhelpline
import org.apache.spark._
import scala.util.matching.Regex

object helplineEmergency {
  def main(arg: Array[String]): Unit = {
  var sparkConf = new SparkConf().setMaster("local").setAppName("Emergency")
  var sc = new SparkContext(sparkConf)
  var Rdd911 = sc.textFile("file:///home/ritish/Downloads/EmergencyHelpline/911.csv")
  var zipRdd = sc.textFile("file:///home/ritish/Downloads/EmergencyHelpline/zipcode.csv")

  var filterRdd1 = Rdd911.filter(x => !x.contains("lat"))
  var newrdd = filterRdd1.filter{ x =>
  val numberPattern: Regex = ",,".r
  numberPattern.findFirstMatchIn(x) == None
  }
  var emergencyData911 = newrdd.map(parse)
  var group911Rdd = emergencyData911.map(w => (w.zip,w.title))

  var filterRdd2 = zipRdd.filter(x => !x.contains("zip"))
  var removequotesrdd = filterRdd2.map(x => x.replace('"', ' ').trim())
  var emergencyData1 = removequotesrdd.map(x => x.replace(" ","").trim())
  var emergencyDatazip = emergencyData1.map(parse2)
  var groupcityzipRdd = emergencyDatazip.map(w =>(w.zip,w.city))
  var groupstatezipRdd = emergencyDatazip.map(w => (w.zip,w.state))
  var combinedRdd = group911Rdd.join(groupstatezipRdd)
  var staterdd = combinedRdd.map{case (a,(b,c)) => (b,c)}
  var stateRddcount = staterdd.map(x => (x._1,x._2)).countByValue()
  //var finalstateRdd = stateRddcount.map{case ((a,b),c) => (f"Crime Type=$a%8s",f" State=$b%4s",f" Crime Count=$c%8s")}.foreach(println)
    
  var groupRdd = group911Rdd.join(groupcityzipRdd)
  var cityrdd = groupRdd.map{case (a,(b,c)) => (b,c)}
  var cityRddcount = cityrdd.map(x => (x._1,x._2)).countByValue()
  println("")
  var finalstateRdd = stateRddcount.map{case ((a,b),c) => (f"Crime Type=$a%8s",f" State=$b%4s",f" Crime Count=$c%8s")}.foreach(println)
  println("")
  var finalcityRdd = cityRddcount.map{case ((a,b),c) => (f"Crime Type=$a%8s",f" City=$b%21s",f" Crime Count=$c%8s")}.foreach(println)
  println("")
  sc.stop()
}
  case class Emergency(latitude: Double, longitude: Double, description: String, zip: Int, title: String, timestamp: String, twp: String, address: String, e:Int)
    def parse(row: String): Emergency = {
    val fields = row.split(",")
    val latitude: Double = fields(0).toDouble
    val longitude: Double = fields(1).toDouble
    val description: String = fields(2)
    val zip: Int = fields(3).toInt
    val title: String = fields(4).substring(0,fields(4).indexOf(":")).toString()
    val timestamp: String = fields(5)
    val twp: String = fields(6)
    val address: String = fields(7)
    val e :Int = fields(8).toInt
    
    Emergency(latitude,longitude,description,zip,title,timestamp,twp,address,e)
    }
  
  case class Emergency2(zip: Int, city: String, state: String,latitude: Double, longitude: Double, timezone: Int, dst: Int)
  def parse2(row: String): Emergency2 = {
    val fields = row.split(",")
    val zip: Int = fields(0).toInt
    val city: String = fields(1)
    val state: String = fields(2)
    val latitude: Double = fields(3).toDouble
    val longitude: Double = fields(4).toDouble
    val timezone: Int = fields(5).toInt
    val dst: Int= fields(6).toInt
    
  Emergency2(zip,city,state,latitude,longitude,timezone,dst)

  }
}
