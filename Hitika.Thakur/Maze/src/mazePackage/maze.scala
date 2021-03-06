package mazePackage

object maze1 {
  def main(Args: Array[String]) {
    var k = 0    
    println("enter the number of rows:")
    var rows = scala.io.StdIn.readInt()
    println("enter the number of coloumns ")
    var coloumns = scala.io.StdIn.readInt()
    var sol = Array.ofDim[Int](rows, coloumns)
    var matrix = Array.ofDim[Int](rows, coloumns)
    var xSource = new Array[Int](rows * coloumns)
    var ySource = new Array[Int](rows * coloumns)
    var xDestni = 0
    var yDestni = 0

    println("enter the matrix data:")
    for (i <- 0 to rows - 1) {
      for (j <- 0 to coloumns - 1) {
        matrix(i)(j) = scala.io.StdIn.readInt()
      }
    }
    println("Your matrix is as follows:")
    for (i <- 0 to rows - 1) {
      for (j <- 0 to coloumns - 1) {
        print(matrix(i)(j) + "\t")
      }
      println("")
    }

    for (i <- 0 to matrix.length - 1) {
      for (j <- 0 to matrix(i).length - 1) {

        if (matrix(i)(j) == 2) {
          xSource(k) = i
          ySource(k) = j
          k = k + 1
        }

        if (matrix(i)(j) == 3) {
          xDestni = i
          yDestni = j

        }

      }

    }
    for (i <- 0 to k - 1) {
      var xsrc = xSource(i)
      var ysrc = ySource(i)
      solveMaze(matrix, xSource(i), ySource(i), rows, coloumns, xDestni, yDestni, xsrc, ysrc)

    }
  }

  def printmaze(sol: Array[Array[Int]], rows: Int, coloumns: Int) {
    println("After maze generation:")
    for (k <- 0 to rows - 1) {
      for (l <- 0 to coloumns - 1) {
        print(sol(k)(l) + "\t")
      }
      println("")
    }
  }
  def isSafe(matrix: Array[Array[Int]], x: Int, y: Int, rows: Int, coloumns: Int): Boolean = {
    return (x >= 0 && x < rows && y >= 0 && y < coloumns && ((matrix(x)(y) == 1) || (matrix(x)(y) == 2)))
  }
  def solveMaze(matrix: Array[Array[Int]], xSource: Int, ySource: Int, rows: Int, coloumns: Int, xDestni: Int, yDestni: Int, xsrc: Int, ysrc: Int): Boolean = {
    var sol = Array.ofDim[Int](rows, coloumns)

    if (solveMazeUtil(matrix, xSource, ySource, sol, rows, coloumns, xsrc, ysrc,xDestni, yDestni ) == false) {
      println("no solution")
      return false
    }
    printmaze(sol, rows, coloumns)
    return true
  }
  
  def solveMazeUtil(matrix: Array[Array[Int]], x: Int, y: Int, sol: Array[Array[Int]], rows: Int, coloumns: Int, xSource: Int, ySource: Int, xDestni: Int, yDestni: Int): Boolean = {
    if (x == xDestni && y == yDestni) {
      sol(x)(y) = 1
      return true
    }

    if (isSafe(matrix, x, y, rows, coloumns) == true) {
      // for printing 1
      sol(x)(y) = 1

      if (xSource <= xDestni && ySource <= yDestni) {
        if (solveMazeUtil(matrix, x + 1, y, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
        if (solveMazeUtil(matrix, x, y + 1, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
      }

      if (xSource >= xDestni && ySource >= yDestni) {
        if (solveMazeUtil(matrix, x, y - 1, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
        if (solveMazeUtil(matrix, x - 1, y, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
      }

      if (xSource >= xDestni && ySource <= yDestni) {
        if (solveMazeUtil(matrix, x, y + 1, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
        if (solveMazeUtil(matrix, x - 1, y, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
      }

      if (xSource <= xDestni && ySource >= yDestni) {
        if (solveMazeUtil(matrix, x + 1, y, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
        if (solveMazeUtil(matrix, x, y - 1, sol, rows, coloumns, xSource, ySource, xDestni, yDestni))
          return true
      }

      sol(x)(y) = 0
      return false
    }
    return false
  }
}