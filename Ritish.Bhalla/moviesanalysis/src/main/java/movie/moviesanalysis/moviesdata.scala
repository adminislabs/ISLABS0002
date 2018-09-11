package movie.moviesanalysis
import org.apache.spark._
import scala.util.matching.Regex

object moviesdata {
  def main(arg: Array[String]): Unit = {
  var sparkConf = new SparkConf().setMaster("local").setAppName("Movie")
 var sc = new SparkContext(sparkConf)
  var movierdd = sc.textFile("file:///home/ritish/Downloads/movie.csv")
  var ratingrdd = sc.textFile("file:///home/ritish/Downloads/rating.csv")
  var filtermovie = movierdd.filter(x => !x.contains("movieId"))
  var filterrating = ratingrdd.filter(x => !x.contains("userId"))
  var parsemovie = filtermovie.map(parse)
  var parserating = filterrating.map(parse2)
  //No. of movies in each year
  var lessmovie = sc.parallelize(parsemovie.take(5000))
  var groupmovies = lessmovie.map(x => (x.title,x)).groupByKey().collect()
  var size_year = groupmovies.map{case (k,v) =>(k,v.size)}
  //var showmovies = size_year.map{case (a,b) => (f"Year = $a%5s",f" Movies in this year = $b%5s")}.foreach(println)
  // Ratings in each year
  var lessrating = sc.parallelize(parserating.take(5000))
  var grouprating = lessrating.map(x => (x.timestamp,x)).groupByKey().collect()
  var size_rating = grouprating.map{case (k,v) => (k,v.size)}
  //var showrating = size_rating.map{case (a,b) => (f"Year = $a%5s",f" Ratings in this year = $b%5s")}.foreach(println)
  // Average rating per movieId
  var groupmovies_rating = lessrating.map(x => (x.movie_id,x.rating)).groupByKey().collect()
  var size_moviesrating = groupmovies_rating.map{case (k,v) => (k,v.size)}
  var sum_moviesrating = groupmovies_rating.map{case (k,v) => (k,v.sum)}
  var ziprdd = size_moviesrating.zip(sum_moviesrating).map{case ((a,b),(c,d)) => (a,d/b)}
  //var showrating_year = ziprdd.map{case (a,b) => (f"Movie_id =$a%8s", f" Average rating per movie = $b%.3f")}.foreach(println)
  // Average rating per userId
  var groupratinguser = lessrating.map(x => (x.user_id,x.rating)).groupByKey().collect()
  var sizeofuserratings = groupratinguser.map{case (k,v) => (k,v.size)}
  var sumofuserratings = groupratinguser.map{case (k,v) => (k,v.sum)}
  var ziprdd1 = sizeofuserratings.zip(sumofuserratings).map{case ((a,b),(c,d)) => (a,d/b)}
  //var showrating_user = ziprdd1.map{case (a,b) => (f"User_id =$a%8s", f" Average rating per user = $b%.3f")}.foreach(println)
  // no. of movies per genre
  var pair_genres= parsemovie.map(x => (x.genres,x.movie_id)).groupByKey().collect()
  var abc = pair_genres.map{case (k,v) => (k,v.size)}
  //var show = abc.map{case (a,b) => (f"Genre =$a%65s",f" No. of movies per this genre=$b%7s")}.foreach(println)
println()
  var showmovies = size_year.map{case (a,b) => (f"Year = $a%5s",f" Movies in this year = $b%5s")}.foreach(println)
println()
  var showrating = size_rating.map{case (a,b) => (f"Year = $a%5s",f" Ratings in this year = $b%5s")}.foreach(println)
println()
  var showrating_year = ziprdd.map{case (a,b) => (f"Movie_id =$a%8s", f" Average rating per movie = $b%.3f")}.foreach(println)
println()
  var showrating_user = ziprdd1.map{case (a,b) => (f"User_id =$a%8s", f" Average rating per user = $b%.3f")}.foreach(println)
println()
  var show = abc.map{case (a,b) => (f"Genre =$a%65s",f" No. of movies per this genre=$b%7s")}.foreach(println)
println()
sc.stop()
  }
case class Movie(movie_id: Int, title: String, genres: String)
  def parse(row: String): Movie ={
  val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
  val movie_id: Int = fields(0).toInt
  val title: String = fields(1).substring(fields(1).length()-6,fields(1).length()-2)
  val genres: String = fields(2).replace('"', ' ').trim()
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
  
