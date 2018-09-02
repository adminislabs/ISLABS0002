package emergency

import org.apache.spark._
import scala.util.matching.Regex

object helpline {
  def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var path = sc.textFile("file:////home/mehak/Downloads/EmergencyHelpline/911.csv")
    var filterrdd = path.filter(x=> !x.contains("lat"))
    // var exp: Regex= "([A-Za-z0-9];,{2,}[A-Za-z0-9])".r
    var exp: Regex= "(,,)".r
    var expression = filterrdd.filter(x=> (exp.findFirstMatchIn(x)==None))
    var parserdd = expression.map(parse)
    var path2 = sc.textFile("file:////home/mehak/Downloads/EmergencyHelpline/zipcode.csv")
    var filter2 = path2.filter(x=> !x.contains("zip"))
    var parserdd2 = filter2.map(parse2)
    var zip1 = parserdd.map(x=> (x.zip,x.title))
    var zip2 = parserdd2.map(x=> (x.zip,x.state))
    var join = zip1.join(zip2).map{case(a,(b,c)) => (c,b)}
    var count_of_state = join.countByValue
    var show = count_of_state.map{case((a,b),c) => (a,b,c)}
    var show_count_of_state = show.map{case(a,b,c)=> (f"State= $a%3s " + f"Problem= $b%8s " + f"Count= $c ")}
    
    var zip3 = parserdd2.map(x=> (x.zip,x.city))
    var join2 = zip1.join(zip3).map{case(a,(b,c)) => (c,b)}
    var count_of_city = join2.countByValue
    var show2 = count_of_city.map{case((a,b),c) => (a,b,c)}
    var show_count_of_city = show2.map{case(a,b,c)=> (f"City= $a%10s " + f"Problem= $b%8s " + f"Count= $c ")}
}
  
  case class emergency(lat: String,lng: String,desc: String,zip: Int,title: String,timeStamp: String,twp: String,addr: String,e: Int)extends Serializable{}
  def parse(row: String) :emergency = {
    
    val field = row.split(",")  
    val lat: String = field(0)   
    val lng: String = field(1)
    val desc: String = field(2)
    val zip: Int = field(3).toInt  
    val title: String = field(4).split(":")(0)
    val timeStamp: String = field(5)   
    val twp: String = field(6)   
    val addr: String = field(7)   
    val e: Int = field(8).toInt
    
    emergency(lat,lng,desc,zip,title,timeStamp,twp,addr,e)
    
  }
  
  case class zipcode(zip: Int,city: String,state: String,latitude: String,longitude: String,timezone: Int,dst: Int)extends Serializable{}
  def parse2(row: String) :zipcode = {
    val row_without_quotes = row.replace("\"", "")
    val field = row_without_quotes.split(",") 
    val zip: Int = field(0).toInt
    val city: String = field(1)
    val state: String = field(2)
    val latitude: String = field(3)
    val longitude: String = field(4)
    val timezone: Int = field(5).toInt
    val dst: Int = field(6).toInt
    
    zipcode(zip,city,state,latitude,longitude,timezone,dst)
  
  }

 }