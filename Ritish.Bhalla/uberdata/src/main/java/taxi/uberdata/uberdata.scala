package taxi.uberdata
import org.apache.spark._
object uberdata {
  def main(arg: Array[String]): Unit = {
  var sparkConf = new SparkConf()
  var sc = new SparkContext(sparkConf)
  var firstrdd = sc.textFile("file:///home/ritish/Downloads/Uber/uber.txt")
  var filterrdd = firstrdd.filter(x => !x.contains("dispatching_base_number"))
  var parserdd = filterrdd.map(parse)
  //for trips in a month
  var group_month = parserdd.map(x => (x.date_month,x.trips)).groupByKey().collect()
  var sum_trips = group_month.map{case (k,v) => (k,v.sum)}
  var nooftrips_month = sum_trips.map{case (a,b) => (f"Month =$a%2s",f" Total Trips in this month =$b%15s")}.foreach(println)
  //for active vehicles in a month
  var group_month1 = parserdd.map(x => (x.date_month,x.active_vehicles)).groupByKey().collect()
  var sum_trips1 = group_month1.map{case (k,v) => (k,v.sum)}
  var noofactivevehicles_month = sum_trips1.map{case (a,b) => (f"Month =$a%2s",f" Total Active vehicles in this month =$b%15s")}.foreach(println)
  // for trips per day
  var group_day = parserdd.map(x => (x.date_day,x.trips)).groupByKey().collect()
  var sum_trips2 = group_day.map{case (k,v) => (k,v.sum)}
  var nooftrips_day = sum_trips2.map{case (a,b) => (f"Day =$a%2s",f" Total Trips on this day =$b%15s")}.foreach(println)
  // for active vehicles per day
  var group_day1 = parserdd.map(x => (x.date_day,x.active_vehicles)).groupByKey().collect()
  var sum_trips3 = group_day1.map{case (k,v) => (k,v.sum)}
  var noofactivevehicles_day = sum_trips3.map{case (a,b) => (f"Day =$a%2s",f" Total Active vehicles on this day =$b%10s")}.foreach(println)
  //for active vehicles per dispatch
  var group_dispatch_active = parserdd.map(x => (x.dispatching_base_number,x.active_vehicles)).groupByKey().collect()
  var sum_trips4 = group_dispatch_active.map{case (k,v) => (k,v.sum)}
  var noofactivevehicles_dispatch = sum_trips4.map{case (a,b) => (f"Dispatch Base no. = $a%2s",f" Total Active vehicles under this dispatch =$b%15s")}.foreach(println)
  // for trips per dispatch
  var group_dispatch_trips  = parserdd.map(x => (x.dispatching_base_number,x.trips)).groupByKey().collect()
  var sum_trips5 = group_dispatch_trips.map{case (k,v) => (k,v.sum)}
  var nooftrips_dispatch = sum_trips5.map{case (a,b) => (f"Dispatch Base no. = $a%2s",f" Total trips under this dispatch no. = $b%10s")}.foreach(println)
  }
    case class Uber(dispatching_base_number: String,date_month: Int,date_day: Int, active_vehicles: Int, trips:Int)
    def parse(row:String): Uber = {
      val fields = row.split(",")
      val dispatching_base_number: String = fields(0)
      val date_month: Int = fields(1).substring(0,1).toInt
      val date_day: Int = fields(1).split("/")(1).toInt
      val active_vehicles: Int = fields(2).toInt
      val trips: Int = fields(3).toInt
      
      Uber(dispatching_base_number,date_month,date_day,active_vehicles,trips)
    }
}
