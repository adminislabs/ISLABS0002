import scala.io.StdIn
case class Book(BookName:String,Author:String,YearOfPub:Int) extends Ordered[Book]{
    def compare(that:Book) = BookName.compare(that.BookName)
    override def toString = "Book Name :" + BookName + "\tAuthor : "+Author+"\tYear of Publication : "+YearOfPub +"\n"
}


object MainClass{
        val TOTAL_ALPHABETS = 26
    
                def main(arg: Array[String]): Unit= {
                    var choice:Int = 1
                    var count:Int = 0
                    var alphabet:Int = 65
                   
                    var bookArray = Array.ofDim[Array[Book]](TOTAL_ALPHABETS)


                     

                         for ( count <- 0 to bookArray.length-1){
                                     println()
                                     println("Enter the number of Books you want to add in "+alphabet.toChar)
                                     var length:Int = scala.io.StdIn.readInt
                                     bookArray(count) = Array.ofDim[Book](length)
                                     addBooks(count,length,bookArray)


                                    
                                     alphabet=alphabet+1
                         }
                     
                     while (choice != 4){
                        println()
                        println("**********************************************************************")
                        println ("Enter 1 to Print all Books.")
                        println ("Enter 2 to Sort Books by Book Name.")
                        println ("Enter 3 to Sort Books by Year.")
                        println ("Enter 4 to Exit.")

                        choice = scala.io.StdIn.readInt

                        choice match {
                                      case 1 => printBooks(bookArray)
                                      case 2 => sortByName(bookArray)
                                      case 3 => sortByYear(bookArray)
                                      case 4 =>   
                                      case everythingElse => println("Please Enter Number between 1 to 4. ")
                                    }

                     }
       

                  }

                def printBooks(bookArray:Array[Array[Book]]):Unit = {
                        println()
                       // Print two dimensional array
                       var alphabet:Int = 65
                  for (i <- 0 to bookArray.length-1) {
                    println(alphabet.toChar)
                     for ( j <- 0 to bookArray(i).length-1) {
                        print(" " + bookArray(i)(j));
                     }
                     println();
                     alphabet=alphabet+1
                  }
                }
                def printBooks(bookArray:Array[Book]):Unit = {
                        println()
                       // Print one dimensional array for printing by year
                      
                  for (i <- 0 to bookArray.length-1) {
                   
                        print(" " + bookArray(i));
                     
                     println();
                    
                  }
                }

                def addBooks(row:Int,rowLength:Int,bookArray:Array[Array[Book]]):Unit = {
                    var bookName:String = ""
                    var Author:String = ""
                    var Year:Int = 0
                    
                    for(i <- 0 to rowLength-1){
                        println()
                        var bookNumber:Int = i+1
                        println("--------------------- Book "+bookNumber+" --------------------")
                        println()
                        print("Enter Book Name : ")
                        bookName = scala.io.StdIn.readLine
                        print("Enter Author Name : ")
                        Author = scala.io.StdIn.readLine
                        print("Enter Year of Publication (YYYY) : ")
                        Year = scala.io.StdIn.readInt
                        

                        bookArray(row)(i) = new Book(bookName,Author,Year)
                    }

                }

                def sortByName(bookArray:Array[Array[Book]]):Unit = {

                         for (i <- 0 to bookArray.length-1) {
                        scala.util.Sorting.quickSort(bookArray(i))
                    }
                    printBooks(bookArray)
                }

                def sortByYear(bookArray:Array[Array[Book]]): Unit = {
                        
                       var TotalBooks:Int = 0
                       var pointer:Int = 0
                       for(i <- 0 to bookArray.length-1){
                           TotalBooks = bookArray(i).length + TotalBooks
                       }
                       var sortedByYear:Array[Book] = Array.ofDim[Book](TotalBooks)

                    for (i <- 0 to bookArray.length-1) {
                     for ( j <- 0 to bookArray(i).length-1) {
                         sortedByYear(pointer) = bookArray(i)(j)
                         pointer = pointer+1
                     }
                    
                  }


                        var final_sortedByYear = sortedByYear.filter(_!=null)
                         final_sortedByYear = final_sortedByYear.sortBy(_.YearOfPub)
                    
                    printBooks(final_sortedByYear)
                }
}
