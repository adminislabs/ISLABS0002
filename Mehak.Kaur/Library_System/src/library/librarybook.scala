//package library
import scala.util.Sorting


case class book(name_of_book: String, author_of_book: String, year_of_publication: Int)

 object byname extends Ordering[book]{
  def compare (a: book, b: book) = a.name_of_book compare b.name_of_book
}
 object byyear extends  Ordering[book]{
   def compare (a: book, b: book) = a.year_of_publication compare b.year_of_publication
 }



  object librarybook {

    var arr1 = new Array[Array[book]](26)

    def main(args: Array[String]) {
      while (true) {
        println("Enter 1 to enter a book")
        println("Enter 2 to search a book")
        println("Enter  3 to sort book by name")
        println("Enter 4 to sort book by year")
        println("Enter 5 to exit")

        var choice = readInt()
        choice match {
          case 1 => add_book()
          case 2 => search_book()
          case 3 => sort_by_name()
          case 4 => sort_by_year()
          case 5 => exit()
        }
      }
    }

    def add_book() {
      println("Enter the alphabet (Strictly UPPER CASE):")
      var alpha = scala.io.StdIn.readChar()

      println("Enter the number of books:")
      var num = scala.io.StdIn.readInt()
      arr1(alpha.toInt - 65)  = new Array[book](num)


      for (i <- 0 to num - 1) {
        println("Enter the full name of book "+(i+1))
        var name = scala.io.StdIn.readLine()

        println("Enter the name of author")
        var author = scala.io.StdIn.readLine()

        println("Enter the year of Publication")
        var year = scala.io.StdIn.readInt()

        arr1(alpha.toInt - 65)(i) = new book(name, author, year)

        println("BOOK ENTERED")
      }


    }

    def search_book() {
      println("Enter the alphabet of book you want to search")
      var search = scala.io.StdIn.readChar()

      println("Enter the full name of book")
      var full_name = scala.io.StdIn.readLine()


      if (arr1(search.toInt - 65) == null) {
        println("NO BOOK EXIST BY THAT NAME")
      }
      else {

        for (i <- 0 to arr1(search.toInt - 65).length - 1) {
          if (arr1(search.toInt - 65)(i).name_of_book == full_name)
            println("BOOK FOUND")
          else {
            println("BOOK NOT FOUND")
          }
        }

      }
    }

    def sort_by_name() {
      var c = 0
       for (i <- 0 to 25){
         if(arr1(i) != null)
           Sorting.quickSort(arr1(i))(byname)
       }
      for(j <- 0 to 25){
        if(arr1(j) != null)
          for(k <- 0 to arr1(j).length - 1) {
            println(arr1(j)(k))
            c = c + 1
          }
      }
     if (c == 0)
       println("Please insert book")

    }

    def sort_by_year() {
      var sum = 0
      for(i <- 0 to 25){
        if(arr1(i) != null) {
          sum = sum + arr1(i).length
        }
      }
      var temp = new Array[book](sum)


      var s = 0
      for(j <- 0 to 25) {
        if (arr1(j) != null) {
          for (k <- 0 to arr1(j).length - 1) {
            temp(s) = arr1(j)(k)
            s = s + 1
          }
        }
      }

      Sorting.quickSort(temp)(byyear)


      for (j <- 0 to temp.length-1){

          println(temp(j))
      }
    }

    def exit() {
      System.exit(1)

    }
  }
