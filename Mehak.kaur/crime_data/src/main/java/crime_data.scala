

object crime_data {
  def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var path = sc.textFile("file:////home/mehak/Downloads/CrimeData/crime_dataset.csv")
    var filterrdd = path.filter(x => !x.contains("id"))
 
}