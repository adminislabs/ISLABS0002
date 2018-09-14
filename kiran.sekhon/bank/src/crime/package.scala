import org.apache.spark._
import scala.util.matching.Regex

package object crime {
  def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var firstRdd = sc.textFile("file:///home/kiran/Downloads/CrimeData/crime.csv")
    var filterRdd = firstRdd.filter(x => !x.contains("latitude"))
    var newData = filterRdd.map(parse)

    var pairrdd = newData.map(x => ((x.fbic), 1)).reduceByKey(_ + _).foreach(println) // (1)no. of crimes under each fbi_code

    var no_of_narcotics_Cases = newData.filter(x => x.primary_type == "NARCOTICS" && x.year == 2015).count // (2). narcotics cases in 2015

    var total_narcotics_cases = newData.filter(x => x.primary_type == "NARCOTICS")
    var narcotics_case_ineach_district = total_narcotics_cases.map(x => x.fbic).countByValue.foreach(println) // (2a) narcotics case in each district

    var true_arrestRdd = newData.filter(x => x.arrest == true)
    var theftInDistrictRdd = true_arrestRdd.filter(x => x.primary_type == "THEFT")
    var theft_ineach_district = theftInDistrictRdd.map(x => x.dist).countByValue.foreach(println) // (3)theft related arrests in each district

  }

  case class crime(ID: Int, case_no: String, date: String, block: String, IUCR: String, primary_type: String, description: String, location_description: String, arrest: Boolean, dom: String, dist: String, beat: Int, wrd: Int, cm: String, fbic: String, X: Int, Y: Int, year: Int, updated_on: String, latitude: Double, longitude: Double, location: String)
  def parse(row: String): crime = {
    val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)
    val ID: Int = fields(0).toInt
    val case_no: String = fields(1)
    val date: String = fields(2)
    val block: String = fields(3)
    val IUCR: String = fields(4)
    val primary_type: String = fields(5)
    fields(6).split("\\s+")
    val description: String = fields(6)
    val location_description: String = fields(7)
    val arrest: Boolean = fields(8).toBoolean
    val dom: String = fields(9)
    val beat: Int = fields(10).toInt
    val dist: String = fields(11)
    val wrd: Int = fields(12).toInt
    val cm: String = fields(13)
    val fbic: String = fields(14)
    if (fields(15) == "") fields(15) = "0000000"
    val X: Int = fields(15).toInt
    if (fields(16) == "") fields(16) = "0000000"
    val Y: Int = fields(16).toInt
    val year: Int = fields(17).toInt
    val updated_on: String = fields(18)
    if (fields(19) == "") fields(19) = "00.00"
    val latitude: Double = fields(19).toDouble
    if (fields(20) == "") fields(20) = "00.00"
    val longitude: Double = fields(20).toDouble
    if (fields(21) == "") fields(21) = "00.00,00.00"
    val location: String = fields(21)

    crime(ID, case_no, date, block, IUCR, primary_type, description, location_description, arrest, dom, dist, beat, wrd, cm, fbic, X, Y, year, updated_on, latitude, longitude, location)

  }

}