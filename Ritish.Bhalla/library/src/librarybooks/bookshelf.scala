package librarybooks
import scala.util.Sorting
import scala.util.control._
case class Books(var name: String, var author: String, var year: Int)

object sortyear extends Ordering[Books] {
  def compare(a: Books, b: Books) = a.year compare b.year
}
object sortname extends Ordering[Books] {
  def compare(a: Books, b: Books) = a.name compare b.name
}

object bookshelf {
  var myarray = new Array[Array[Books]](26)

  def main(Args: Array[String]) {
    while (true) {
      println("Enter your choice")
      println("1 for adding a new book")
      println("2 for searching the name of the book")
      println("3 for sorting the book by name")
      println("4 for sorting the book by year")
      println("5 for exit")
      var choice = scala.io.StdIn.readInt()

      choice match {
        case 1 => add_book()
        case 2 => search_book()
        case 3 => sort_by_name()
        case 4 => sort_by_year()
        case 5 => System.exit(0)
      }
    }
  }
  def add_book() {

    println("Enter the first alphabet of the book in capital letter:")
    var first_alphabet_book = scala.io.StdIn.readChar()

    println("Enter the number of books you want to add:")
    var no_books = scala.io.StdIn.readInt()

    //for converting ASCII values
    myarray(first_alphabet_book.toInt - 65) = Array.ofDim[Books](no_books)

    for (i <- 0 to no_books - 1) {
      println("Enter book number " + (i + 1) + " full name")
      var full_name = scala.io.StdIn.readLine()

      println("Enter the author's name: ")
      var author_name = scala.io.StdIn.readLine()

      println("Enter the publication year of the book:")
      var yearofpublication = scala.io.StdIn.readInt()
      myarray(first_alphabet_book.toInt - 65)(i) = new Books(full_name, author_name, yearofpublication)
    }
    println("Books inserted!!!")
  }

  def search_book() {
    var d = 0
    println("Enter the first alphabet of the book in capital letter:")
    var firstalphabet = scala.io.StdIn.readChar()

    println("Enter the name of the book:")
    var fullname = scala.io.StdIn.readLine()

    if (myarray(firstalphabet.toInt - 65) == null) {
      println("There is no such book")
    } else {

      for (j <- 0 to myarray(firstalphabet.toInt - 65).length - 1) {

        if (myarray(firstalphabet.toInt - 65)(j).name == fullname) {
          println("Book found")
          d = d + 1
        }
      }
      if (d == 0) {
        println("Not Found")
      }
    }
  }

  def sort_by_name() {
    var c = 0
    for (i <- 0 to 25) {
      if (myarray(i) != null)
        Sorting.quickSort(myarray(i))(sortname)
    }
    for (j <- 0 to 25) {
      if (myarray(j) != null) {
        for (k <- 0 to myarray(j).length - 1) {
          println(myarray(j)(k))
          c = c + 1
        }
      }
    }
    if (c == 0) {
      println("Please add book first by pressing 1")
    }
  }
  def sort_by_year() {
    var c = 0
    var sum = 0

    for (k <- 0 to 25) {
      if (myarray(k) != null)
        sum = sum + myarray(k).length
    }
    var arr = new Array[Books](sum)
    for (i <- 0 to 25) {
      if (myarray(i) != null) {
        for (s <- 0 to myarray(i).length - 1) {
          arr(c) = myarray(i)(s)

          c = c + 1
        }
      }
    }
    Sorting.quickSort(arr)(sortyear)
    // for printing the sorted array
    for (i <- 0 to sum - 1) {
      println(arr(i))
    }
    if (c == 0) {
      println("Please add book first by pressing 1")
    }
  }
}
