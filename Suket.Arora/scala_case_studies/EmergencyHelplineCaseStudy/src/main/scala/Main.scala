import org.apache.spark._
import scala.math.BigDecimal
import scala.io.Source
import java.sql.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._                   //import Window library
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._


object Main extends App {


  override def main(arg: Array[String]): Unit = {
  

   var sparkConf = new SparkConf().setMaster("local").setAppName("EmergencyAnalytics")
   var sc = new SparkContext(sparkConf)
       val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
      

 // val RddRaw = sc.textFile("file:///home/suket/case_studies/EmergencyHelplineCaseStudy/src/main/resources/zipcode.csv") 

val customSchema = StructType(Array(
        StructField("zip", IntegerType, true),
        StructField("city", StringType, true),
        StructField("state", StringType, true),
        StructField("latitude", FloatType, true),
        StructField("longitude", FloatType, true),
        StructField("timezone", IntegerType, true),
        StructField("dst", StringType, true)

      ))

    // # File location and type
val file_location = "file:///home/suket/case_studies/EmergencyHelplineCaseStudy/src/main/resources/zipcode.csv"
val file_type = "csv"

// # CSV options
val infer_schema = "true"
val first_row_is_header = "true"
val delimiter = ","

// # The applied options are for CSV files. For other file types, these will be ignored.
val zips = spark.read.format(file_type) 
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header) 
  .option("sep", delimiter) 
  .schema(customSchema)
  .load(file_location)
 

 val file_location2 = "file:///home/suket/case_studies/EmergencyHelplineCaseStudy/src/main/resources/911.csv"
// val file_type = "csv"



// # The applied options are for CSV files. For other file types, these will be ignored.
val code911 = spark.read.format(file_type) 
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header) 
  .option("sep", delimiter) 
  .option("mode", "DROPMALFORMED")
  // .option("timestampFormat", "yyyy-MM-dd hh:mm:ss")
  
  .load(file_location2)

   import spark.implicits._
 


import org.apache.spark.sql.functions.{ udf, col }
def substringFn(string: String) : String= {
    val str = string.trim
    val problem = str.split(':')
    var str2 = problem(0)
    str2
}
val substring = udf(substringFn _)
val code911WithProblem = code911.withColumn("Problem", substring(col("title")))   


 val Full_Data = code911WithProblem.join (zips, Seq("zip"))

  
   
   val CrimesInEachState = Full_Data.groupBy("state").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} 

   var CrimesStateWise:Map[String,Array[(String,Long)]] = Map()

    for ( count <- 0 to CrimesInEachState.length-1){
        CrimesStateWise+=( CrimesInEachState(count)._1 -> Full_Data.filter($"state" === CrimesInEachState(count)._1).groupBy($"Problem").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} )

    }    


       val CrimesInEachCity = Full_Data.groupBy("city").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} 

   var CrimesCityWise:Map[String,Array[(String,Long)]] = Map()

    for ( count <- 0 to CrimesInEachCity.length-1){
        CrimesCityWise+=( CrimesInEachCity(count)._1 -> Full_Data.filter($"city" === CrimesInEachCity(count)._1).groupBy($"Problem").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} )

    }                                                                                       
     
    CrimesStateWise foreach {case (key, value) => println(value.foreach(print)  + " Emergencies in "+key)}

     println()
    
    CrimesCityWise foreach {case (key, value) => println(value.foreach(print)  + " Emergencies in "+key)}


/*             // ---------------------------   Finds only most Frequent Crime ---------------------------
 def windowSpec = Window.partitionBy("state", "Problem") 

   val MostPrevelantProbleminEachstate /*: Array[(String, String)] */=  Full_Data.withColumn("count", count("Problem").over(windowSpec))     // counting repeatition of Problem for each group of state, Problem and assigning that Problem to new column called as count
                                                                      .orderBy($"count".desc)                                   // order dataframe with count in descending order
                                                                      .groupBy("state")                                           // group by state
                                                                      .agg(first("Problem").as("Problem"))                         //taking the first row of each key with count column as the highest
                                                                      .collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[String])}                        
    
   def windowSpec2 = Window.partitionBy("city", "Problem") 

   val MostPrevelantProbleminEachCity /*: Array[(String, String)]*/ =  Full_Data.withColumn("count", count("Problem").over(windowSpec2))     // counting repeatition of Problem for each group of state, Problem and assigning that Problem to new column called as count
                                                                      .orderBy($"count".desc)                                   // order dataframe with count in descending order
                                                                      .groupBy("city")                                           // group by state
                                                                      .agg(first("Problem").as("Problem"))                         //taking the first row of each key with count column as the highest
                                                                      .collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[String])} 

*/
 


 sc.stop()
    }

}



