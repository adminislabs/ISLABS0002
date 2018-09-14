object maze_solver {
  def main(args: Array[String]): Unit = {

    println("Enter number of rows:")
    var row = scala.io.StdIn.readInt()

    println("Enter number of columns:")
    var col = scala.io.StdIn.readInt()

    var array = Array.ofDim[Int](row, col)
    var sourcex = new Array[Int](row * col)
    var sourcey = new Array[Int](row * col)
    var k = 0
    var destinationx = 0
    var destinationy = 0

    println("Enter your data:")

    for (i <- 0 to row - 1) {
      for (j <- 0 to col - 1) {
        array(i)(j) = scala.io.StdIn.readInt()
        if (array(i)(j) == 2) {
          sourcex(k) = i

          sourcey(k) = j
          k = k + 1
        }
        if (array(i)(j) == 3) {
          destinationx = i
          destinationy = j
        }

      }
    }

    println("Your matrix looks like:")
    for (i <- 0 to row - 1) {
      for (j <- 0 to col - 1) {
        print(array(i)(j) + "\t")
      }
      println("")
    }

    for (i <- 0 to k - 1) {
      var xst = sourcex(i)
      var yst = sourcey(i)
      solveMaze(array, row, col, sourcex(i), sourcey(i), destinationx, destinationy, xst, yst)

    }
  }

  def printSolution(sol: Array[Array[Int]], row: Int, col: Int) {
    println("here is solution")
    for (i <- 0 to row - 1) {
      for (j <- 0 to col - 1) {
        print(sol(i)(j) + "\t")
      }
      println("")
    }

  }

  def isSafe(array: Array[Array[Int]], x: Int, y: Int, row: Int, col: Int): Boolean = {

    return (x >= 0 && x < row && y >= 0 && y < col && (array(x)(y) == 1) || (array(x)(y) == 2))

  }

  def solveMaze(array: Array[Array[Int]], row: Int, col: Int, sourcex: Int, sourcey: Int, destinationx: Int, destinationy: Int, xst: Int, yst: Int): Boolean = {
    val sol = Array.ofDim[Int](row, col)
    if (solveMazeUtil(array, row, col, sourcex, sourcey, sol, destinationx, destinationy, xst, yst) == false) {
      println("Solution does not exist")
      return false
    }
    printSolution(sol, row, col)
    return true
  }

  def solveMazeUtil(array: Array[Array[Int]], row: Int, col: Int, x: Int, y: Int, sol: Array[Array[Int]], destinationx: Int, destinationy: Int, xst: Int, yst: Int): Boolean = {
    if (x == destinationx && y == destinationy) {
      sol(x)(y) = 1
      return true
    }
    if (isSafe(array, x, y, row, col) == true) {
      sol(x)(y) = 1
      
      if (xst <= destinationx && yst <= destinationy) {
        if (solveMazeUtil(array, row, col, x + 1, y, sol, destinationx, destinationy, xst, yst))
          return true
        if (solveMazeUtil(array, row, col, x, y + 1, sol, destinationx, destinationy, xst, yst))
          return true
      }
      
      if (xst >= destinationx && yst >= destinationy) {
        if (solveMazeUtil(array, row, col, x - 1, y, sol, destinationx, destinationy, xst, yst))
          return true
        if (solveMazeUtil(array, row, col, x, y - 1, sol, destinationx, destinationy, xst, yst))
          return true
      }
      
      if (xst >= destinationx && yst <= destinationy) {
      if (solveMazeUtil(array, row, col, x -1, y, sol, destinationx, destinationy, xst, yst))
          return true
        if (solveMazeUtil(array, row, col, x, y + 1, sol, destinationx, destinationy, xst, yst))
          return true
        
      }
      
      if (xst <= destinationx && yst >= destinationy) {
        if (solveMazeUtil(array, row, col, x+1, y, sol, destinationx, destinationy, xst, yst))
          return true
        if (solveMazeUtil(array, row, col, x, y-1 , sol, destinationx, destinationy, xst, yst))
          return true
      }
      
      

      sol(x)(y) = 0
      return false
    }

    return false
  }
}

