package emergency.helpline

import org.apache.spark._

import scala.util.matching.Regex

object helpline
{
def main(arg: Array[String]): Unit = {
 var sparkConf = new SparkConf().setMaster("local").setAppName("Emergency")
 var sc = new SparkContext(sparkConf)

  var rdd911 = sc.textFile("file:///home/hitika/Downloads/EmergencyHelpline/911.csv")
  var ziprdd = sc.textFile("file:///home/hitika/Downloads/EmergencyHelpline/zipcode.csv")
  var filterrdd911 = rdd911.filter(x =>  !x.contains("lat"))
   var filterziprdd  = ziprdd .filter(x =>  !x.contains("zip"))
   
   // using regular expressions for removing spaces
   
    var newrdd911 = filterrdd911.filter{ x =>
  val numberPattern: Regex = ",,".r
  numberPattern.findFirstMatchIn(x) == None
  }
 var parserdd911 = newrdd911.map(parse)
 
   var newrddzip = filterziprdd.map(x => x.replace('"',' ').trim())
   var newrddzip1 = newrddzip.map(x => x.replace(" ","").trim())
   var parserddzip = newrddzip1.map(parse2)
  
   var zipsortrdd = parserddzip.map(x => (x.zip, x.state))
   var sort911rdd = parserdd911.map(x => (x.zip, x.title))
   var combordd = zipsortrdd.join(sort911rdd)
   var removebracerdd = combordd.map{case(a,(b,c)) => (b,c)}
 var countcrimerdd = removebracerdd.map(x => (x._1, x._2 )).countByValue()

 
 
var zipsortcity = parserddzip.map(x => (x.zip, x.city))
var combocityrdd = zipsortcity.join(sort911rdd)
 var removebracecity = combocityrdd.map{case(a,(b,c)) => (b,c)}
var countcitycrime = removebracecity.map(x => (x._1, x._2)).countByValue()

println()

var finalstaterdd = countcrimerdd.map{case((a,b),c) => (f"State = $a%3s ",   f" Crime =$b%9s ",  f" CrimeCount=$c%7s ")}.foreach(println)
println()
var finalcityrdd = countcitycrime.map{case((a,b),c) => (f"City = $a%22s",  f" Crime =$b%10s ", f" Crime Count= $c%8s ")}.foreach(println)
println()

  
 sc.stop()  
}


case class Emergency (latitude:Double,longitude:Double, description:String, zip:Int, title:String, timeStamp:String ,twp:String, address:String, e:Int)

def parse(row: String): Emergency={
  
  val fields = row.split(",")
  val latitude: Double = fields(0).toDouble
  val longitude: Double = fields(1).toDouble
  val description: String = fields(2)
  val zip: Int = fields(3).toInt
  val title: String = fields(4).substring(0, fields(4).indexOf(":")).toString()
  val timeStamp:String = fields(5) 
  val twp:String = fields(6)
  val address:String = fields(7)
  val e:Int = fields(8).toInt
  
  Emergency (latitude,longitude, description, zip, title, timeStamp ,twp, address, e)
  
}
  
case class zipcode(zip:Int,city:String,state:String,latitude:Double ,longitude:Double,timezone:Int,dest:Int)

def parse2(row: String): zipcode={
  val fields = row.split(",")
	val zip:Int = fields(0).toInt
	val city:String = fields(1)
  val state: String = fields(2)
	val latitude: Double = fields(3).toDouble
  val longitude: Double = fields(4).toDouble
	val timezone: Int = fields(5).toInt
	val dest: Int = fields(6).toInt
		  
  zipcode(zip,city,state,latitude ,longitude,timezone,dest)

}

}

