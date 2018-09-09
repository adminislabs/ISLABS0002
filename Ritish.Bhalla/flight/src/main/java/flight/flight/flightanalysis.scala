package flight.flight
import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalTime
import org.joda.time.LocalDate
import org.apache.spark._
import scala.util.matching.Regex
object flightanalysis {
  def main(arg: Array[String]): Unit = {
 var sparkConf = new SparkConf().setMaster("local").setAppName("Flight")
  var sc = new SparkContext(sparkConf)
 var firstRdd = sc.textFile("file:///home/ritish/Downloads/flight-delays/flights.csv")
 var filterRdd = firstRdd.filter(x => !x.contains("YEAR"))
 var newrdd = filterRdd.filter{x =>
 val numberPattern: Regex = "[0-9A-Za-z],{2,}[0-9A-Za-z]".r
 numberPattern.findFirstMatchIn(x) == None
 }
 var flightData1 = newrdd.map(parse)
 //var totalDistance = flightData1.map(_.distance).reduce((x, y) => x + y)
 var totalflights = flightData1.count()
 var depDelay = flightData1.filter(x =>(x.dep_dely>0))
 var depDelayCount = depDelay.count
 //var arrDelay = flightData1.filter(x =>(x.ar_delay>0))
 //var arrDelayCount = arrDelay.count
 var depDelaypercent = (depDelayCount*100.0)/totalflights
 //var arrDelaypercent = (arrDelayCount*100.0)/totalflights
 var averageDepDelay = depDelay.map(x => x.dep_dely) 
 val sumadely = averageDepDelay.reduce(_+_)
 var averageofDepDelay = sumadely/depDelayCount
 //var avgarrDel = arrDelay.map(x => x.ar_delay)
 //val sumarrdely = avgarrDel.reduce(_+_)
 //var averageofarrDelay = sumarrdely/arrDelayCount
 var flightdata = sc.parallelize(flightData1.take(580000))
 var groupRdd = flightdata.map(w =>(w.airline,w)).groupByKey().collect
 var noofdelayedflightofairlines = groupRdd.map{ case (key,value) => (key,value.filter(_.dep_dely>0))}
 var countofdelayedflightofairlines = noofdelayedflightofairlines.map{ case (key,value) => (key,value.size)}.toList
 var noofflightofairlines = groupRdd.map{ case (key,value) => (key,value.size)}.toList
 var thirdRdd = noofflightofairlines.zip(countofdelayedflightofairlines).map {case ((a,b),(c,d)) => (a,b,d)}
 var fourthRdd = thirdRdd.map{case (a,b,c) =>(a,b,c,(c*100.0)/b)}
 var sumRdd = groupRdd.map{ case (key,value) => (key,value.filter(_.dep_dely>0))}.toList
 var abc = sumRdd.map{ case (key,value) => (key,value.map(_.dep_dely).sum)}.toList
 var dd=countofdelayedflightofairlines.zip(abc)
 var dd1=dd.map{ case ((a,b),(c,d))=>(a,d/b)}

println("")
println("Total Number of Flights = " +totalflights) 
println("Delayed Flights =" +depDelayCount)
println("Percentage of delayed flights = " +depDelaypercent+ " %")
println("Average Delay Time = " +averageofDepDelay)
println("")
var finalRdd = fourthRdd.zip(dd1).map{case ((a,b,c,d),(e,f)) => (f"Airline = $a%29s",f" Total Flights = $b%7s",f" Delayed Flights= $c%6s",f" Delay Percentage = $d%.2f"+" %",f" Average Delay = $f%.2f")}.foreach(println)
println("")
sc.stop()
}
case class Flight(date: LocalDate, airline: String, flightNumber: String, origin: String,dest: String, dep: LocalTime, dep_dely: Double, arv: LocalTime,ar_delay: Double, airtime: Double, distance: Double) extends Serializable {}
def parse(row: String): Flight = {
 val fields = row.split(",")
 val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
 val timepattern = DateTimeFormat.forPattern("HHmm")
 val date: LocalDate = datePattern.parseDateTime(fields(0)+"-"+fields(1)+"-"+fields(2)).toLocalDate()
 if (fields(4)=="UA") fields(4)="United Air Lines Inc."
 else if (fields(4)=="AA") fields(4)="American Airlines Inc."
 else if (fields(4)=="US") fields(4)="US Airways Inc."
 else if (fields(4)=="F9") fields(4)="Frontier Airlines Inc."
 else if (fields(4)=="B6") fields(4)="JetBlue Airways"
 else if (fields(4)=="OO") fields(4)="Skywest Airlines Inc."
 else if (fields(4)=="AS") fields(4)="Alaska Airlines Inc."
 else if (fields(4)=="NK") fields(4)="Spirit Air Lines"
 else if (fields(4)=="WN") fields(4)="Southwest Airlines Co."
 else if (fields(4)=="DL") fields(4)="Delta Air Lines Inc."
 else if (fields(4)=="EV") fields(4)="Atlantic Southeast Airlines"
 else if (fields(4)=="HA") fields(4)="Hawaiian Airlines Inc."
 else if (fields(4)=="MQ") fields(4)="American Eagle Airlines Inc."
 else if (fields(4)=="VX") fields(4)="Virgin America"
 val airline: String = fields(4)
 val flightNumber: String = fields(5)
 val origin: String = fields(7)
 val dest: String = fields(8)
 if(fields(10)=="2400") fields(10)="0000"
 val dep: LocalTime = timepattern.parseLocalTime(fields(10))
 val dep_dely: Double = fields(11).toDouble
 if(fields(21)=="2400") fields(21)="0000"
 val arv: LocalTime = timepattern.parseLocalTime(fields(21))
 val ar_delay: Double = fields(22).toDouble
 val airtime: Double = fields(16).toDouble
 val distance: Double = fields(17).toDouble

 Flight(date, airline, flightNumber, origin, dest, dep, dep_dely, arv, ar_delay, airtime, distance)
}
}
