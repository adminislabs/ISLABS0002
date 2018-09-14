import org.apache.spark._
import scala.util.matching.Regex

package object emergency_helpline {
  def main(args: Array[String]) = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var firstRdd = sc.textFile("file:///home/kiran/Downloads/EmergencyHelpline/911.csv")
    var secondRdd = sc.textFile("file:///home/kiran/Downloads/EmergencyHelpline/zipcode.csv")
    var filterRdd1 = firstRdd.filter(x => !x.contains("lat"))
    var filterRdd2 = secondRdd.filter(x => !x.contains("zip"))
    var expression: Regex = "(,,)".r
    var rdd = filterRdd1.filter(x => (expression.findFirstMatchIn(x) == None))
    var newData1 = rdd.map(parse)
    var newData2 = filterRdd2.map(parse1)
    var join1 = newData2.map(x => (x.zip, x.state))
    var join2 = newData2.map(x => (x.zip, x.city))
    var join3 = newData1.map(x => (x.zip, x.title))
    var staterdd = join1.join(join3).map { case (a, (b, c)) => (b, c) }
    var count1 = staterdd.countByValue.map { case ((a, b), c) => (a, b, c) }
    var state_rdd = count1.map { case (a, b, c) => (f"STATE=$a%5s " + f"PROBLEMS=$b%5s " + f"COUNT= $c") } // no. of crimes prevalent in each state
    var cityrdd = join2.join(join3).map { case (a, (b, c)) => (b, c) }
    var count2 = cityrdd.countByValue.map { case ((a, b), c) => (a, b, c) }
    var city_rdd = count2.map { case (a, b, c) => (f"CITY=$a%5s " + f"PROBLEMS=$b%5s " + f"COUNT= $c") } // no.of crimes prevalent in each city

  }

  case class Emergency(latitude: Double, longitude: Double, zip: Int, title: String, time_stamp: String, twp: String, address: String)

  def parse(row: String): Emergency = {
    val fields = row.split(",")
    val latitude: Double = fields(0).toDouble
    val longitude: Double = fields(1).toDouble
    val zip: Int = fields(3).toInt
    val title: String = fields(4).split(":")(0)
    val time_stamp: String = fields(5)
    val twp = fields(6)
    val address: String = fields(7)

    Emergency(latitude, longitude, zip, title, time_stamp, twp, address)
  }

  case class Emergency_helpline(zip: Int, city: String, state: String, latitude: Double, longitude: Double, time_zone: Int)

  def parse1(row: String): Emergency_helpline = {
    val fields = row.split(",")
    fields(0) = fields(0).replace("\"", "")
    val zip: Int = fields(0).toInt
    val city: String = fields(1)
    val state: String = fields(2)
    fields(3) = fields(3).replace("\"", "")
    val latitude: Double = fields(3).toDouble
    fields(4) = fields(4).replace("\"", "")
    val longitude: Double = fields(4).toDouble
    fields(5) = fields(5).replace("\"", "")
    val time_zone: Int = fields(5).toInt
    Emergency_helpline(zip, city, state, latitude, longitude, time_zone)
  }

}