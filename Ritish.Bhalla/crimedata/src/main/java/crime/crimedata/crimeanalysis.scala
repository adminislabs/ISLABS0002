package crime.crimedata
import org.apache.spark._
object crimeanalysis {
  def main(arg: Array[String]): Unit = {

  var sparkConf = new SparkConf().setMaster("local").setAppName("flight")
  var sc = new SparkContext(sparkConf)
  var firstRdd = sc.textFile("file:///home/ritish/Downloads/CrimeData/crimedata.csv")
  var crimeRdd =firstRdd.map(parse)
  //var np = crimeRdd.map(x => (x.fbicode,1)).reduceByKey(_+_)
  // crime under each fbi code
  var groupRdd = crimeRdd.map(w =>(w.fbicode,w)).groupByKey().collect
  var no_ofcrimes_fbicode = groupRdd.map{case (key,value) => (key,value.size)}
  var show_fbi = no_ofcrimes_fbicode.map{case (a,b) => (f"Fbi Code =$a%5s", f"No. of Crimes=$b%5s")}
  // narcotics cases in 2015
  var noof_narcotics_cases = crimeRdd.filter(x => (x.crimetype == "NARCOTICS" && x.year == 2015)).count()
  println("No. of Narcotics cases in 2015 = "+noof_narcotics_cases)
  // no. of narcotics case under fbi code in district
  var filternarcotics = crimeRdd.filter(x => x.crimetype == "NARCOTICS")
  var noofnarcotics_each_district = filternarcotics.map(x => x.fbicode).countByValue()
  var shownarcocrime  = noofnarcotics_each_district.map{case (a,b) => (f"District =$a%5s",f"No. of Narcotics cases=$b%5s")}.foreach(println)
  //for district related theft
  var districtRdd = crimeRdd.filter(x => (x.arrest == "true"))
  var district_theftRdd = districtRdd.filter(x => x.crimetype == "THEFT")
  var noof_districttheft = district_theftRdd.map(x => x.district).countByValue()
  var showtheftarrest = noof_districttheft.map{case (a,b) => (f"District =$a%5s",f"No. of Theft related arrests=$b%5s")}.foreach(println)
  // for ward
  var noof_wardcasecaught = crimeRdd.map(x =>x.ward).countByValue()
  var showwardcases = noof_wardcasecaught.map{case (a,b) => (f"Ward =$a%5s",f"No. of Crime cases=$b%5s")}.foreach(println)

  // for community
  var noof_communitycaught = crimeRdd.map(x =>x.community).countByValue()
  var showcommunitycases = noof_communitycaught.map{case (a,b) => (f"Community =$a%5s",f"No. of Crime cases=$b%5s")}.foreach(println)

  sc.stop()
  }
  case class Crime(id: Int,casenumber: String,date: String,block: String,iucr: String,crimetype: String,descriptionofcrime: String,locationcrime: String,arrest: String,domestic: String,beat: Int,district: Int,ward: Int,community: Int,fbicode: String,xcord: Int,ycord: Int,year: Int,updated_on: String,latitude: Double,longitude: Double,location: String)
  def parse(row: String): Crime={
    val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
    val id: Int = fields(0).toInt
    val casenumber: String = fields(1)
    val date: String = fields(2)
    val block: String = fields(3)
    val iucr: String = fields(4)
    val crimetype: String = fields(5)
    fields(6).split("\\s+")
    val descriptionofcrime: String = fields(6)
    val locationcrime: String = fields(7)
    val arrest: String = fields(8)
    val domestic: String = fields(9)
    val beat: Int = fields(10).toInt
    val district :Int = fields(11).toInt
    val ward :Int = fields(12).toInt
    val community :Int = fields(13).toInt
    val fbicode :String = fields(14)
    if (fields(15)=="") fields(15)="0000000"
    val xcord: Int = fields(15).toInt
    if (fields(16)=="") fields(16)="0000000"
    val ycord: Int = fields(16).toInt
    val year: Int = fields(17).toInt
    val updated_on: String = fields(18)
    if (fields(19)=="") fields(19)="00.000000000"
    val latitude: Double = fields(19).toDouble
    if (fields(20)=="") fields(20)="00.000000000"
    val longitude: Double = fields(20).toDouble
    if (fields(21)=="") fields(21)="00.000000000, 00.000000000"
    val location: String = fields(21)
    
    Crime(id,casenumber,date,block,iucr,crimetype,descriptionofcrime,locationcrime,arrest,domestic,beat,district,ward,community,fbicode,xcord,ycord,year,updated_on,latitude,longitude,location)
  }
}
