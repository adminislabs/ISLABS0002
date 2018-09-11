
package crimedata
import org.apache.spark._


object crimepetroldelta{
   def main(args: Array[String]): Unit = {
   var sparkConf = new SparkConf().setMaster("local").setAppName("Transaction")
   var sc = new SparkContext(sparkConf)
   var saverdd = sc.textFile("file:///home/hitika/Downloads/CrimeData/crime_data.csv")
   var parserdd = saverdd.map(parse)
   var fbirdd = parserdd.map(x => (x.Fbicode,x))
   var crimetyperdd = fbirdd.mapValues(x => (x.Primary_type)).countByValue()
   //var finalrdd = crimetyperdd.map{case ((a,b),c) => (f"FBI Code= $a%4s " ,f"Crime Type = $b%40s ",f"Count = $c%7s")}
   
   var yearrdd = parserdd.map(x => (x.Year, x.Primary_type)).countByValue()
   //var finalyearrdd = yearrdd.map{case ((a,b),c) => (f"YEAR= $a%4s ",f"Crime Type = $b%40s ",f"Count = $c%7s")}
   
   var disttrdd = parserdd.filter(x => (x.Arrest == "true"))
   var thirdrdd = disttrdd.filter(x =>(x.Primary_type =="THEFT")) 
   var mapdistt = thirdrdd.map(x => (x.District )).countByValue()
   println()
   var finalrdd = crimetyperdd.map{case ((a,b),c) => (f"FBI Code= $a%4s " ,f"Crime Type = $b%40s ",f"Count = $c%7s")}.foreach(println)
   println()
   var finalyearrdd = yearrdd.map{case ((a,b),c) => (f"YEAR= $a%4s ",f"Crime Type = $b%40s ",f"Count = $c%7s")}.foreach(println)
   println()
   var district_theft = mapdistt.map{case (a,b) => (f"District = $a%5s",f" Theft related arrests = $b%5s")}.foreach(println)
   println()
   sc.stop()
   
   }
  
  case class Crime (id: Int,Case_Number: String ,Date: String , Block:String , IUCR: String ,Primary_type: String ,Description:String,
Location_description:String,Arrest: String ,Domestic: String ,Beat: Int, District: Int ,Ward: Int ,community: Int ,Fbicode:String,Year:Int,Updatedon:String ,loctation:String )
      def parse(row: String): Crime={
      val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
      val id : Int = fields(0).toInt
      val Case_Number: String = fields(1)
      val Date: String = fields(2)
      val Block: String = fields(3)
      val IUCR: String = fields(4)
      val Primary_type: String = fields(5)
      val Description: String = fields(6)
      val Location_description: String = fields(7)
      val Arrest: String = fields(8)
      val Domestic: String = fields(9)
      val Beat: Int = fields(10).toInt
      val District: Int = fields(11).toInt
      val Ward: Int= fields(12).toInt
      val community: Int = fields(13).toInt
      val Fbicode: String = fields(14)
      val Year: Int = fields(17).toInt
      val Updatedon: String = fields(18)
      
      if(fields(21)=="") fields(21)="00.000000000, 00.000000000"
     // if(fields(21)==",") fields(21)="0"

      val loctation: String = fields(21)
    
    Crime(id,Case_Number,Date , Block, IUCR,Primary_type,Description,Location_description,Arrest,Domestic,Beat, District,Ward,community,Fbicode,Year,Updatedon,loctation)
    
  }
  }




