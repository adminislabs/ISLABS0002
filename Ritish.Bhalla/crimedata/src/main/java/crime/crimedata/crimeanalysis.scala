package crime.crimedata
import org.apache.spark._
object crimeanalysis {
  def main(arg: Array[String]): Unit = {
  var sparkConf = new SparkConf()
  var sc = new SparkContext(sparkConf)
  var firstRdd = sc.textFile("file:///home/ritish/Downloads/CrimeData/crimedata.csv")
  var crimeRdd =firstRdd.map(parse)
  //var np = crimeRdd.map(x => (x.fbicode,1)).reduceByKey(_+_)
  // crime under each fbi code
  var groupRdd = crimeRdd.map(w =>(w.fbicode,w)).groupByKey().collect
  var no_ofcrimes_fbicode = groupRdd.map{case (key,value) => (key,value.size)}
  // narcotics cases in 2015
  var noof_narcotics_cases = crimeRdd.filter(x => (x.crimetype == "NARCOTICS" && x.year == 2015)).count() 
    // no. of narcotics case under fbi code
  var filternarcotics = crimeRdd.filter(x => x.crimetype == "NARCOTICS")
  var noofnarcotics_each_district = filternarcotics.map(x => x.fbicode).countByValue()
  //for district related theft
  var districtRdd = crimeRdd.filter(x => (x.arrest == "true"))
  var district_theftRdd = districtRdd.filter(x => x.crimetype == "THEFT")
  var noof_districttheft = district_theftRdd.map(x => x.district).countByValue()
  // for ward
  var noof_wardcasecaught = crimeRdd.map(x =>x.ward).countByValue()
  // for community
  var noof_communitycaught = crimeRdd.map(x =>x.community).countByValue()
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
