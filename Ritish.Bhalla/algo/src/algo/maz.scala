package algo

object maz {
  def main(Args: Array[String]) {
    var k = 0
    println("enter the number of rows:")
    var rows = scala.io.StdIn.readInt()
    println("enter the number of coloumns ")
    var coloumns = scala.io.StdIn.readInt()
    var sol = Array.ofDim[Int](rows, coloumns)
    var arr = Array.ofDim[Int](rows, coloumns)
    var xstart = new Array[Int](rows * coloumns)
    var ystart = new Array[Int](rows * coloumns)
    var xend = 0
    var yend = 0
    println("enter the matrix data:")
    for (i <- 0 to rows - 1) {
      for (j <- 0 to coloumns - 1) {
        arr(i)(j) = scala.io.StdIn.readInt()
      }
    }
    println("Your matrix is as follows:")
    for (i <- 0 to rows - 1) {
      for (j <- 0 to coloumns - 1) {
        print(arr(i)(j) + "\t")
      }
      println("")
    }
    for (i <- 0 to arr.length - 1) {
      for (j <- 0 to arr(i).length - 1) {
        if (arr(i)(j) == 2) {

          xstart(k) = i
          ystart(k) = j
          k = k + 1
        }
        if (arr(i)(j) == 3) {
          xend = i
          yend = j
        }
      }
    }
    for (i <- 0 to k - 1) {
      var xst=xstart(i)
      var yst=ystart(i)
      solveMaze(arr, xstart(i), ystart(i), rows, coloumns, xend, yend,xst,yst)
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
  
  def isSafe(arr: Array[Array[Int]], x: Int, y: Int, rows: Int, coloumns: Int): Boolean = {
    return (x >= 0 && x < rows && y >= 0 && y < coloumns && ((arr(x)(y) == 1) || (arr(x)(y) == 2)))
  }
  def solveMaze(arr: Array[Array[Int]], xstart: Int, ystart: Int, rows: Int, coloumns: Int, xend: Int, yend: Int,xst:Int,yst:Int): Boolean = {
    var sol = Array.ofDim[Int](rows, coloumns)

    if (solveMazeUtil(arr, xstart, ystart, sol, rows, coloumns,xst,yst, xend, yend) == false) {
      println("no solution")
      return false
    }
    printmaze(sol, rows, coloumns)
    return true
  }
  def solveMazeUtil(arr: Array[Array[Int]], x: Int, y: Int, sol: Array[Array[Int]], rows: Int, coloumns: Int,xstart:Int,ystart:Int, xend: Int, yend: Int): Boolean = {
    if (x== xend && y == yend) {
      sol(x)(y) = 1
      return true
    }

    if (isSafe(arr, x, y, rows, coloumns) == true) {
      sol(x)(y) = 1
      
      if(xstart<=xend && ystart<=yend ) {
        if(solveMazeUtil(arr, x + 1, y + 0, sol, rows, coloumns,xstart,ystart, xend, yend))
        return true
        if(solveMazeUtil(arr, x + 0, y + 1, sol, rows, coloumns,xstart,ystart, xend, yend))
        return true
      }
      if(xstart>=xend && ystart>=yend) {
        if(solveMazeUtil(arr, x + 0, y - 1, sol, rows, coloumns,xstart,ystart, xend, yend))
        return true
        if(solveMazeUtil(arr, x - 1, y + 0, sol, rows, coloumns, xstart,ystart,xend, yend))
        return true
      }
        
      if (xstart>=xend && ystart<=yend){
         if(solveMazeUtil(arr, x + 0, y + 1, sol, rows, coloumns, xstart,ystart,xend, yend))
        return true
        if(solveMazeUtil(arr, x - 1, y + 0, sol, rows, coloumns,xstart,ystart, xend, yend))
        return true
      }
      if(xstart<=xend && ystart>=yend){
       
        if(solveMazeUtil(arr, x + 1, y + 0, sol, rows, coloumns, xstart,ystart,xend, yend))
        return true
        if(solveMazeUtil(arr, x + 0, y - 1, sol, rows, coloumns,xstart,ystart, xend, yend))
        return true
      }
      sol(x)(y) = 0
      return false
    }
    return false
  }
}