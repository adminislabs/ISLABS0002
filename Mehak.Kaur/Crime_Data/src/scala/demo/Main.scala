package scala.demo

import org.apache.spark._
import scala.util.matching.Regex

object Main {
   def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var path = sc.textFile("file:////home/mehak/Downloads/CrimeData/crime_dataset.csv")
    var filterrdd = path.filter(x => !x.contains("id"))
    //var exp: Regex = "([a-zA-Z0-9],{2,}[a-zA-Z0-9]{2,})".r
    //var expression = filterrdd.filter(x => (exp.findFirstMatchIn(x) == None))
    var parserdd = filterrdd.map(parse)
    //var pairrdd = newrdd.map(x=> ((x.airline),x))
    //var pairrdd = parserdd.map(x => ((x.FBI_code),x))
    //var crimerdd = parserdd.map(_.FBI_code).reduce((x,y)=>(x+y))
    var pairrdd = parserdd.map(x=> ((x.FBI_code),x))
    var fbi_count = parserdd.map(w => ((w.FBI_code),1)).reduceByKey(_+_)
    var show_fbi_count = fbi_count.map{case(a,b) => (f"FBI=$a%4s " +f"Crime count=$b ")}
    var loc_count = parserdd.map(w => ((w.Primary_type),1)).reduceByKey(_+_)
    var show_loc_count = loc_count.map{case(a,b) => (f"Primary type=$a%32s " +f"count=$b ")}
    var narcotics = parserdd.filter(_.Primary_type == "NARCOTICS")
    narcotics.count()
    var pairrdd2 = parserdd.map(x=> ((x.District),x))
    var join = pairrdd2.mapValues(x => (x.Primary_type,x.Arrest)).map{case(a,(b,c)) => (a,b,c)}
    var newnew = join.filter(x=> x._2=="THEFT" && x._3==true)
    var finalrdd = newnew.map(x =>(x._1,(x._2,x._3))).countByKey()
    var show_finalrdd = finalrdd.map{case(a,b) => (f"District=$a%4s " + f"Theft arrest=$b ")}
        
}
   
   
   case class crime(id: Int,case_number: String,Date: String,Block: String,Primary_type: String,Description: String,Location_description: String,Arrest: Boolean,Domestic: Boolean,District: Int,Ward: Int,FBI_code: String, X_coordinate: Int,Y_coordinate: Int,Year: Int,Latitude: String,Longitude: String) extends Serializable{} 
   def parse(row: String) :crime = {
     val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)

     
     val id: Int = fields(0).toInt
     
     val case_number: String = fields(1)
     
     val Date: String = fields(2)
     
     val Block: String = fields(3)
     
     val Primary_type: String = fields(5)
    
     val Description: String = fields(6)
     
     val Location_description: String = fields(7)
     
     val Arrest: Boolean = fields(8).toBoolean
     
     val Domestic: Boolean = fields(9).toBoolean
     
     val District: Int = fields(11).toInt
     
     val Ward: Int = fields(12).toInt
     
     val FBI_code: String = fields(14)
     
     if (fields(15)=="") fields(15)="0000"
     val X_coordinate: Int = fields(15).toInt
     
     if (fields(16)=="") fields(16)="0000"
     val Y_coordinate: Int = fields(16).toInt
     
     val Year: Int = fields(17).toInt
     
     if (fields(19)=="") fields(19)="0000"
     val Latitude: String = fields(19)
     
     if (fields(20)=="") fields(19)="0000"
     val Longitude: String = fields(20)
     
         
     crime(id,case_number,Date,Block,Primary_type,Description,Location_description,Arrest,Domestic,District,Ward,FBI_code,X_coordinate,Y_coordinate,Year,Latitude,Longitude)
   }
   
}