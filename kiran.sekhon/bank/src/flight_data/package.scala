import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalTime
import org.joda.time.LocalDate
import org.apache.spark._
import scala.util.matching.Regex

package object flight_data {
  def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var firstRdd = sc.textFile("file:///home/kiran/flights.csv")
    var filterRdd = firstRdd.filter(x => !x.contains("YEAR"))
    var expression: Regex = "([A-Za-z0-9],{2,}[A-Za-z0-9])".r
    var rdd = filterRdd.filter(x => (expression.findFirstMatchIn(x) == None))
    var flightData = rdd.map(parse)
    var depdelay = flightData.filter(x => x.dep_dely > 0)
    var arrdely = flightData.filter(x => x.ar_delay > 0)
    var percentage_depdelay = (depdelay.count * 100 / rdd.count)
    var percentage_arrdelay = (arrdely.count * 100 / rdd.count)
    var r = depdelay.map(x => x.dep_dely)
    var sum_of_depdelay = r.reduce(_ + _)
    var avg_dep_delay = (sum_of_depdelay / depdelay.count)
    var s = arrdely.map(x => x.ar_delay)
    var sum_of_arrdelay = s.reduce(_ + _)
    var avg_arr_delay = (sum_of_arrdelay / arrdely.count)
    var pairrdd = flightData.map(x => ((x.airline), 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).foreach(println)
    var USdata = flightData.filter(_.airline == "US")
    var pairrdd1 = flightData.map(x => ((x.airline), x))
    var p = pairrdd1.mapValues(x => (x.ar_delay, x.distance, 1)).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3))
    var totalDistance = flightData.map(_.distance).reduce((x, y) => x + y)
    var flightCount = flightData.map(_.dep_dely > 0).count().toDouble
    
  }
  case class Flight(date: LocalDate, airline: String, flightNumber: String, origin: String, dest: String, dep: LocalTime, dep_dely: Double, arv: LocalTime, ar_delay: Double, airtime: Double, distance: Double) extends Serializable {}
  def parse(row: String): Flight = {
    val fields = row.split(",")
    val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
    val timepattern = DateTimeFormat.forPattern("HHmm")
    val date: LocalDate = datePattern.parseDateTime(fields(0) + "-" + fields(1) + "-" + fields(2)).toLocalDate()
    val airline: String = fields(4)
    val flightNumber: String = fields(5)
    val origin: String = fields(7)
    val dest: String = fields(8)
    if (fields(10) == "2400") fields(10) = "0000"
    val dep: LocalTime = timepattern.parseLocalTime(fields(10))
    val dep_dely: Double = fields(11).toDouble
    if (fields(21) == "2400") fields(21) = "0000"
    val arv: LocalTime = timepattern.parseLocalTime(fields(21))
    val ar_delay: Double = fields(22).toDouble
    val airtime: Double = fields(16).toDouble
    val distance: Double = fields(17).toDouble

    Flight(date, airline, flightNumber, origin, dest, dep, dep_dely, arv, ar_delay, airtime, distance)
  }

}