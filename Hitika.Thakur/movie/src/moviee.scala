package movie.moviesanalysis
import org.apache.spark._
import scala.util.matching.Regex

object moviesdata {
 def main(arg: Array[String]): Unit = {
 var sparkConf = new SparkConf().setMaster("local").setAppName("Movie")
 var sc = new SparkContext(sparkConf)
 var movierdd = sc.textFile("file:///home/hitika/Downloads/movie.csv")
 var ratingrdd = sc.textFile("file:///home/hitika/Downloads/rating.csv")
 var filtermovie = movierdd.filter(x => !x.contains("movieId"))
 var filterrating = ratingrdd.filter(x => !x.contains("userId"))
 var parsemovie = filtermovie.map(parse)
 var parserating = filterrating.map(parse2)

 var moviedata = sc.parallelize(parsemovie.take(4000))
 var groupofmovies = moviedata.map(x => (x.title,x)).groupByKey().collect()
 var size_year = groupofmovies.map{case (k,v) =>(k,v.size)}
 //var showmoviesdata = size_year.map{case (a,b) => (f"Year = $a%6s",f" Movies in this year = $b%6s")}

 var ratingdata = sc.parallelize(parserating.take(4000))
 var groupofrating = ratingdata.map(x => (x.timestamp,x)).groupByKey().collect()
 var size_rating = groupofrating.map{case (k,v) => (k,v.size)}
 //var showratingdata = size_rating.map{case (a,b) => (f"Year = $a%6s",f" Ratings in this year = $b%6s")}

 var groupmoviesforrating = ratingdata.map(x => (x.movie_id,x.rating)).groupByKey().collect()
 var sizeofmoviesrating = groupmoviesforrating.map{case (k,v) => (k,v.size)}
 var sumofmoviesrating = groupmoviesforrating.map{case (k,v) => (k,v.sum)}
 var combinerdd = sizeofmoviesrating.zip(sumofmoviesrating).map{case ((a,b),(c,d)) => (a,d/b)}
 //var showratingforyear = combinerdd.map{case (a,b) => (f"Movie_id =$a%8s", f" Average rating =$b%4s")}

 var groupratingforuser = ratingdata.map(x => (x.user_id,x.rating)).groupByKey().collect()
 var sizeforratings = groupratingforuser.map{case (k,v) => (k,v.size)}
 var sumofratings = groupratingforuser.map{case (k,v) => (k,v.sum)}
 var ziprdd = sizeforratings.zip(sumofratings).map{case ((a,b),(c,d)) => (a,d/b)}
 //var showratingforuser = ziprdd.map{case (a,b) => (f"User_id =$a%8s", f" Average rating per user =$b%4s")}
 var showmoviesdata = size_year.map{case (a,b) => (f"Year = $a%6s",f" Movies in this year = $b%6s")}.foreach(println)
 var showratingdata = size_rating.map{case (a,b) => (f"Year = $a%6s",f" Ratings in this year = $b%6s")}.foreach(println)
 var showratingforyear = combinerdd.map{case (a,b) => (f"Movie_id =$a%8s", f" Average rating =$b%4s")}.foreach(println)
var showratingforuser = ziprdd.map{case (a,b) => (f"User_id =$a%8s", f" Average rating per user =$b%4s")}.foreach(println)
 sc.stop()
 }
case class Movie(movie_id: Int, title: String, genres: String)
 def parse(row: String): Movie ={
 val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
 val movie_id: Int = fields(0).toInt
 val title: String = fields(1).substring(fields(1).length()-6,fields(1).length()-2)
 val genres: String = fields(2)
 Movie(movie_id,title,genres)
}
case class Rating(user_id: Int, movie_id: Int, rating: Float,timestamp: String)
 def parse2(row: String): Rating ={
 val fields = row.split(",")
 val user_id: Int = fields(0).toInt
 val movie_id: Int = fields(1).toInt
 val rating: Float = fields(2).toFloat
 val timestamp: String = fields(3).substring(0,4).toString()
 Rating(user_id,movie_id,rating,timestamp)
}
}
