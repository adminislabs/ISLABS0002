package librarySystem
import scala.util.Sorting

case class book(var name: String, var author: String, var year: Int)

object ByName extends Ordering[book] {
  def compare(obj1: book, obj2: book) = obj1.name compare obj2.name
}

object ByYear extends Ordering[book] {
  def compare(obj3: book, obj4: book) = obj3.year compare obj4.year

}
object libraryRoom {

  var arr = Array.ofDim[Array[book]](26)

  def main(args: Array[String]) {

    while (true) {
      println("Enter your choice")
      println("1: Add Name Of Book")
      println("2: Search Name Of The Book")
      println("3:Sort Book by name")
      println("4:Sort by Year")
      println("5:exit")
      var choice = scala.io.StdIn.readInt()

      choice match {

        case 1 => addBook()
        case 2 => Search()
        case 3 => SortByName()
        case 4 => YearPublishing()
        case 5 => System.exit(0)

      }

    }
  }
  def addBook() {
    // var i=0
    println("Enter The Alphabet You Want To Add")
    var firstAlphabet = scala.io.StdIn.readChar()

    println("Enter how many books you want To add")
    var No_of_Books = scala.io.StdIn.readInt()
    arr(firstAlphabet.toInt - 65) = Array.ofDim[book](No_of_Books)

    for (i <- 0 to No_of_Books - 1) {
      println("Enter book number" + (i + 1) + " Name of book")

      var NameOfBook = scala.io.StdIn.readLine()

      println("Enter Author Name")
      var Author_Name = scala.io.StdIn.readLine()

      println("Enter year of publish")
      var Year0fPublishing = scala.io.StdIn.readInt()
      arr(firstAlphabet.toInt - 65)(i) = new book(NameOfBook, Author_Name, Year0fPublishing)
    }
    println("Your records have been inserted")

  }

  def Search() {
    var c = 0
    println("enter alphabet of the book you want to enter")
    var firstAlphabet1 = scala.io.StdIn.readChar()
    // arr(firstAlphabet1.toInt-65)=Array.ofDim[book](No_of_Books)

    println("enter name of the book")
    var NameOfBook1 = scala.io.StdIn.readLine()

    if (arr(firstAlphabet1.toInt - 65) == null) {
      println("Null encountered so there is no such book")

    } else {
      for (j <- 0 to arr(firstAlphabet1.toInt - 65).length - 1) {

        if (arr(firstAlphabet1.toInt - 65)(j).name == NameOfBook1) {
          println(" book found")
          c = c + 1
        }
      }
      if (c == 0) {
        println(" book not found")

      }
    }
  }

  def SortByName() {
    var c = 0
    for (i <- 0 to 25) {
      if (arr(i) != null)
        Sorting.quickSort(arr(i))(ByName)
    }
    for (j <- 0 to 25) {
      if (arr(j) != null) {
        for (k <- 0 to arr(j).length - 1) {
          println(arr(j)(k))
          c = c + 1
        }

      }
    }
    if (c == 0) {
      println("insert book")
    }
  }
  def YearPublishing() {
    var sum = 0
    var m = 0

    for (i <- 0 to 25) {
      if (arr(i) != null)
        sum = sum + arr(i).length

    }
    if (sum == 0) {
      println("Please insert book")
    }

    var temp = new Array[book](sum)

    for (j <- 0 to 25) {
      if (arr(j) != null) {
        for (k <- 0 to arr(j).length - 1) {
          temp(m) = arr(j)(k)
          m = m + 1

        }
      }
    }
    Sorting.quickSort(temp)(ByYear)
    for (i <- 0 to sum - 1) {
      println(temp(i))
    }

  }

}