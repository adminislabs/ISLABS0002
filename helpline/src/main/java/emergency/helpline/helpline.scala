package emergency.helpline

import org.apache.spark._
object helpline
{
def main(arg: Array[String]): Unit = {
 var sparkConf = new SparkConf()
 var sc = new SparkContext(sparkConf)
  var firstRdd = sc.textFile("file:///home/hitika/Downloads/EmergencyHelpline/911.csv")
  var secondRdd = sc.textFile("file:///home/hitika/Downloads/EmergencyHelpline/zip.csv")
}


case class Emergency (latitude:Double,longitude:Double, description:String, zip:Int, title:String, timeStamp:String ,twp:String, address:String, e:Int)

def parse(row: String): Emergency={
  
  val fields = row.split(",")
  val latitude: Double= fields(0)
  val longitude: Double= fields(1)
  val description: String= fields(2)
  val zip:Int = fields(3)
  val title: String= fields(4)
  val timeStamp:String = fields(5) 
  val twp:String= fields(6)
  val address:String = fields(7)
  val e:Int = fields(8)
  
  Emergency (latitude,longitude, description, zip, title, timeStamp ,twp, address, e)
  
}
  
case class zipcode(zip:Int,city:String,state:String,latitud:Double ,longitud:Double,timezone:Int,dest:Int)

def parse2(row: String): zipcode={
  
  val fields= row.split(",")
		  val zip: Int= fields(0)
		  val city: String= fields(1)
		  val state: String= fields(2)
		  val latitud: Double= fields(3)
		  val longitud: Double= fields(4)
		  val timezone: Int= fields(5)
		  val dest: Int= fields(6)
}

}


