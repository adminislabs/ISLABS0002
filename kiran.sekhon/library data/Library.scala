import scala.util.Sorting
import scala.util.control._
case class Book(name_of_book: String, author_of_book: String, year_of_publication: Int)
object byname extends Ordering[Book] {

 def compare(a: Book, b: Book) = a.name_of_book compare b.name_of_book
}
object byyear extends Ordering[Book] {
 def compare(a: Book, b: Book) = a.year_of_publication compare b.year_of_publication
}

object library {

 var arr = new Array[Array[Book]](26)

 def main(args: Array[String]) {
   while (true) {
     println("Enter 1 to add a book")
     println("Enter 2 to search a book")
     println("Enter 3 to sort by name")
     println("Enter 4 to sort by year of publishing")
     println("Enter 5 to exit")

     var book = readInt()
     book match {
       case 1 => add_book()
       case 2 => search_book()
       case 3 => sort_by_name()
       case 4 => sort_by_year()
       case 5 => exit()
     }
   }
 }
 def add_book() {
   var loop = new Breaks
   println("Enter the alphabet in upper case:")
   var alpha = scala.io.StdIn.readChar()

   println("Enter the number of books:")
   var num = scala.io.StdIn.readInt()

   arr(alpha.toInt - 65) = new Array[Book](num)

   for (i <- 0 to num - 1) {

     println("Enter the name of the book with first alphabet in capital:")
     var name = scala.io.StdIn.readLine()

     if (name(0).toInt - 65 == alpha.toInt-65) {
       println("enter the name of author:")
       var authorname = scala.io.StdIn.readLine()

       println("enter the year of publishing:")
       var year = scala.io.StdIn.readInt()

       arr(alpha.toInt - 65)(i) = new Book(name, authorname, year)

       println("Task completed")
     } else {
       println("Try again with correct input")
     }

   }
 }
 def search_book() {
   println("Enter the first alphabet")
   var first_alpha = scala.io.StdIn.readChar()

   println("Enter the name of book:")
   var book_name = scala.io.StdIn.readLine()

   if (arr(first_alpha.toInt - 65) == null) {
     println("There is no such book")
   } else {

     for (j <- 0 to arr(first_alpha.toInt - 65).length - 1) {
       if (arr(first_alpha.toInt - 65)(j).name_of_book == book_name)
         println("FOUND")

       else {
         println("NOT FOUND")
       }
     }
   }
 }

 def sort_by_name() {
   var f = 0

   for (i <- 0 to 25) {
     if (arr(i) != null)
       Sorting.quickSort(arr(i))(byname)
   }
   for (j <- 0 to 25) {
     if (arr(j) != null)
       for (k <- 0 to arr(j).length - 1) {
         println(arr(j)(k))
         f = f + 1

       }
   }
   if (f == 0) {
     println("Insert book")
   }

 }

 def sort_by_year() {
   var sum = 0
   for (i <- 0 to 25) {
     sum = sum + arr(i).length
   }
   var temp = new Array[Book](sum)
   var a = 0
   for (j <- 0 to 25) {
     if (arr(j)!= null) {
     for (k <- 0 to arr(j).length - 1) {
       temp(a) = arr(j)(k)
       a = a + 1
     }
   }
   }
 Sorting.quickSort(temp)(byyear)
 for (a <- 0 to temp.length-1)
   println(temp(a))

 }

 def exit() {
   System.exit(1)
 }
}
