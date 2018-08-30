import scala.io.StdIn

import scala.util.control._
object Puzzle{

    var matrix:Array[Array[Int]] = Array.empty
     def main(arg: Array[String]): Unit= {
        // create a Breaks object as follows
        val loop = new Breaks;
        print("Enter number of rows = ")
        var rows = readInt
        println()
        print("Enter number of columns = ")
        var columns = readInt
        println()
        var x = 0
        var y = 0
        var flag = 0
        val outer = new Breaks; //object for break
        val inner = new Breaks; //object for break
        var Answer=Array.ofDim[Int](rows,columns)
        matrix = Array.ofDim[Int](rows,columns)
        println("Enter Matrix like ")

       println("Example") 

            println("1 1 1 1 1")
            println("1 2 0 1 2")
            println("1 1 0 1 2")
            println("1 1 0 3 2")
            println("1 1 1 1 1")

            
            println("0 way to move")
            println("1 way to wall")
            println("2 means enter")
            println("3 means exit")
            println()

      for ( i <- 0 to rows-1){
            for ( j <- 0 to columns-1){
                matrix(i)(j) = readInt
            }
     }
     printMatrix(matrix)
        outer.breakable // Outer Block
        {
              for ( i <- 0 to rows-1){
                 inner.breakable // Inner Block
                {
                    for ( j <- 0 to columns-1){
                        if (matrix(i)(j) == 2 ){
                            x=i
                            y=j
                            flag = 1
                             inner.break;
                        }
                        }
                        if (flag == 1){
                             outer.break;
                        }
                        
                    }// inner breakable

             }
         
         }// outer breakable.
     println()

        var path = shortestPath2(matrix,Answer,x,y,rows-1,columns-1)
//        printMatrix(path)
        if (path.isEmpty){
          println("No Solution Found.")
        }

    }
    def printMatrix( matrix:Array[Array[Int]]):Unit={
                     println()
                       // Print two dimensional array
                      
                  for (i <- 0 to matrix.length-1) {
                    
                     for ( j <- 0 to matrix(i).length-1) {
                        print(" " + matrix(i)(j));
                     }
                     println();
                     
                  }
    }

    


       def shortestPath2(m:Array[Array[Int]],ans:Array[Array[Int]],x:Int,y:Int,ex:Int,ey:Int):Array[Array[Int]] = {
       
        
          if (matrix(x)(y) == 1 || matrix(x)(y) == -2 ) return ans
         
          else if (matrix(x)(y) == 3){
            matrix(x)(y) = -2
            ans(x)(y) = 1
            printMatrix(ans)
//             matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }
        else if(y==0 && x==0){
             matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x,y+1,ex,ey) 
                           
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans

        }
        else if (x==0 && y==ey){
             matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }
        else if (y==0 && x==ex){
             matrix(x)(y) = -2
             ans(x)(y) = 1
                           
                             
                            shortestPath2(m,ans,x,y+1,ex,ey) 
                            shortestPath2(m,ans,x-1,y,ex,ey)
                            
                           
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans

        }
         else if(y==0){
             matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x,y+1,ex,ey) 
                            shortestPath2(m,ans,x-1,y,ex,ey) 
                            
                           
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans

        }
        else if(x==0){
            matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x,y+1,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }
        else if (x==ex && y==ey){
            matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x-1,y,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }

        else if(x==ex){
           matrix(x)(y) = -2
           ans(x)(y) = 1
             
                            
                            shortestPath2(m,ans,x,y+1,ex,ey)
                            shortestPath2(m,ans,x-1,y,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }
        else if(y==ey){
             matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x-1,y,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans

        }
        
       
        else{
            matrix(x)(y) = -2
             ans(x)(y) = 1
                            shortestPath2(m,ans,x+1,y,ex,ey) 
                            shortestPath2(m,ans,x,y+1,ex,ey) 
                            shortestPath2(m,ans,x-1,y,ex,ey) 
                            shortestPath2(m,ans,x,y-1,ex,ey)
            matrix(x)(y) = 0
             ans(x)(y) = 0
            ans
        }
    }
}