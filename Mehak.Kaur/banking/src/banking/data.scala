package banking

import org.apache.spark._

object data {
  def main(arg: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var path = sc.textFile("file:////home/mehak/Downloads/banking data/Transaction Sample data-1 (copy).txt")
    var filter = path.filter(x=> !x.contains("Name"))
    var path2 = sc.textFile("file:////home/mehak/Downloads/banking data/Transaction Sample data-2 (copy).txt")
    var filter2 = path2.filter(x=> !x.contains("Name"))
    var parserdd1 = filter.map(parse)
    var parserdd2 = filter2.map(parse2)
    
    var file1_c = parserdd1.filter(x=> (x.c_d == 'C'))                                 //calculating avg of credit from file 1
    var pairrdd = file1_c.map(x=> (x.c_d,x.amount)).groupByKey()
    var count1_c = pairrdd.map{case(a,b)=> (a,b.size)}
    var sum_c = pairrdd.map{case(a,b) => (a,b.sum)}
    var avg1 = sum_c.zip(count1_c).map{case((a,b),(c,d)) => (a,b/d)}
    //var file1_c_count = file1_c.count
    //var count_c = file1_c.map(_.amount).reduce((x,y) => x+y)
    //var avg1 = count_c/file1_c_count
    
   
    var file2_c = parserdd2.filter(x=> (x.c_d == 'C'))                                   //calculating avg of credit from file 2
    var pairrdd2 = file2_c.map(x=> (x.c_d,x.amount)).groupByKey()
    var count2_c = pairrdd2.map{case(a,b)=> (a,b.size)}
    var sum2_c = pairrdd2.map{case(a,b) => (a,b.sum)}
    var avg2 = sum2_c.zip(count2_c).map{case((a,b),(c,d)) => (a,b/d)}
    //var file2_c_count = file2_c.count
    //var count2_c = file2_c.map(_.amount).reduce((x,y) => x+y)
    //var avg2 = count2_c/file2_c_count
    
    var final_avg_credit = avg1.zip(avg2).map{case((a,b),(c,d)) => (a,(b+d)/2)}             //final avg of credit from file 1 and file 2
    var final_show_credit = final_avg_credit.map{case(a,b) => (f"Average Credit($a)= " + f"$b ")}
    //var final_avg_c = (avg1+avg2)/2
    
    var file1_d = parserdd1.filter(x=> (x.c_d == 'D'))                                      // calculating avg of debit from file 1
    var pairrdd3 = file1_d.map(x=> (x.c_d,x.amount)).groupByKey()
    var count1_d = pairrdd3.map{case(a,b)=> (a,b.size)}
    var sum1_d = pairrdd3.map{case(a,b) => (a,b.sum)}
    var avg3 = sum1_d.zip(count1_d).map{case((a,b),(c,d)) => (a,b/d)}
    //var file1_d_count = file1_d.count
    //var count_d = file1_d.map(_.amount).reduce((x,y) => x+y)
    //var avg3 = count_d/file1_d_count
    
    
   var file2_d = parserdd2.filter(x=> (x.c_d =='D'))                                         //calculating avg of debit from file 2
   var pairrdd4 = file2_d.map(x=> (x.c_d,x.amount)).groupByKey()
    var count2_d = pairrdd4.map{case(a,b)=> (a,b.size)}
    var sum2_d = pairrdd4.map{case(a,b) => (a,b.sum)}
    var avg4 = sum2_d.zip(count2_d).map{case((a,b),(c,d)) => (a,b/d)}
   //var file2_d_count = file2_d.count
   //var count2_d = file2_d.map(_.amount).reduce((x,y) => x+y))
   //var avg4 = count2_d/file2_d_count
   //var final_avg_d = (avg3+avg4)/2
    
    var final_avg_debit = avg3.zip(avg4).map{case((a,b),(c,d)) => (a,(b+d)/2)}               //total avg of debit from both the files  
    var final_show_debit = final_avg_debit.map{case(a,b) => (f"Average Debit($a)= " + f"$b ")}
    
    var filterc = parserdd1.filter(x=> x.c_d=='C')                                        //year wise transaction of credit from file 1
    var zip1 = filterc.map(x=> (x.timestamp,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
    var zip2 = filterc.map(x=> (x.timestamp,x.amount)).groupByKey().map(x=> (x._1,x._2.size))
    var year_avg_transaction = zip1.zip(zip2).map{case((a,b),(c,d)) => (a,b/d)}
   
    
    var filter2_c = parserdd2.filter(x=> x.c_d=='C')                                       //year wise transaction of credit from file 2
    var zip5 = filter2_c.map(x=> (x.timestamp,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
    var zip6 = filter2_c.map(x=> (x.timestamp,x.amount)).groupByKey().map(x=> (x._1,x._2.size))
    var year_avg_transaction_c = zip1.zip(zip2).map{case((a,b),(c,d)) => (a,b/d)}
     
    var avg_credit_transaction= year_avg_transaction.zip(year_avg_transaction_c).map{case((a,b),(c,d)) => (a,(b+d)/2)} //avg of credit transaction
     
    var show_avg_credit_transaction = avg_credit_transaction.map{case(a,b) => (f"Year= $a%4s " + f"Average Transaction(in Dollars)= $b ")}
    
    
    var filterd = parserdd1.filter(x=> x.c_d=='D')                                         //year wise transaction of debit from file 1
    var zip3 = filterd.map(x=> (x.timestamp,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
    var zip4 = filterd.map(x=> (x.timestamp,x.amount)).groupByKey().map(x=> (x._1,x._2.size))
    var year_avg_transaction_d = zip3.zip(zip4).map{case((a,b),(c,d)) => (a,b/d)}
    var show_year_avg_transaction_d = year_avg_transaction_d.map{case(a,b) => (f"Year= $a%4s " + f"Average Transaction(in Dollars)= $b ")}
    
    
    var filter2_d = parserdd2.filter(x=> x.c_d=='D')                                         //year wise transaction of debit from file 2
    var zip7 = filter2_d.map(x=> (x.timestamp,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
    var zip8 = filter2_d.map(x=> (x.timestamp,x.amount)).groupByKey().map(x=> (x._1,x._2.size))
    var year_avg_transaction_d_2 = zip7.zip(zip8).map{case((a,b),(c,d)) => (a,b/d)}
     
    var avg_debit_transaction= year_avg_transaction_d.zip(year_avg_transaction_d_2).map{case((a,b),(c,d)) => (a,(b+d)/2)}//avg of debit transaction

    var show_avg_debit_transaction = avg_debit_transaction.map{case(a,b) => (f"Year= $a%4s " + f"Average Transaction(in Dollars)= $b ")}
    
    var c_1 = parserdd1.map(x => (x.account_id,x)).groupByKey().map{case(a,b) => (a,b.filter(_.c_d == 'C').map(_.amount))}
    var c_1_sum = c_1.map(x => (x._1,x._2.sum))
    
    var c_2 = parserdd2.map(x => (x.account_id,x)).groupByKey().map{case(a,b) => (a,b.filter(_.c_d == 'C').map(_.amount))}
    var c_2_sum = c_2.map(x => (x._1,x._2.sum))
    
    var combined_c_sum = c_1_sum.zip(c_2_sum).map{case ((a,b),(c,d)) => (a,(b+d))}
    
    var d_1 = parserdd1.map(x => (x.account_id,x)).groupByKey().map{case(a,b) => (a,b.filter(_.c_d == 'D').map(_.amount))}
    var d_1_sum = d_1.map(x => (x._1,x._2.sum))
    
    var d_2 = parserdd2.map(x => (x.account_id,x)).groupByKey().map{case(a,b) => (a,b.filter(_.c_d == 'D').map(_.amount))}
    var d_2_sum = d_2.map(x => (x._1,x._2.sum))
    
    var combined_d_sum = d_1_sum.zip(d_2_sum).map{case ((a,b),(c,d)) => (a,(b+d))}
    
    var balance = combined_c_sum.zip(combined_d_sum).map{case ((a,b),(c,d)) => (a,(b-d))}
    var max = balance.max()(new Ordering[Tuple2[Int, Int]]() {
     override def compare(x: (Int, Int), y: (Int, Int)): Int =
       Ordering[Int].compare(x._2, y._2)
           })
          
   
   // var parse_c1 = parserdd1.filter(x=> (x.c_d=='C'))
   //var new1 = parse_c1.map(x=>(x.account_id,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
   //var parse_c2 = parserdd2.filter(x=> (x.c_d=='C'))
   //var new2 = parse_c2.map(x=>(x.account_id,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
   //var zipzip = new1.zip(new2).map{case ((a,b),(c,d))=> (a,(b+d))}
    //var parse_d1 = parserdd1.filter(x=> (x.c_d=='D'))
    //var new3 = parse_d1.map(x=>(x.account_id,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
   //var parse_d2 = parserdd2.filter(x=> (x.c_d=='D'))
    //var new4 = parse_d2.map(x=>(x.account_id,x.amount)).groupByKey().map(x => (x._1,x._2.sum))
    //var zipzipzip = new3.zip(new4).map{case ((a,b),(c,d))=> (a,(b+d))}
    //var balance = zipzip.zip(zipzipzip).map{case ((a,b),(c,d)) => (a,(b-d))}
    
    
}
  
  case class transaction1(account_id: Int,name: String,timestamp: String,c_d: Char,amount: Int)
  def parse(row: String) :transaction1 = {
    
    val field = row.split(",")
    
    val account_id: Int = field(0).toInt
    val name: String = field(1)
    val timestamp: String = field(2).split("/")(2)
    val c_d: Char = field(3).toCharArray()(0)
    val amount: Int =  field(4).replace("$","").replaceAll(" ","").toInt
      
    transaction1(account_id,name,timestamp,c_d,amount)
    
  }
  
  case class transaction2(account_id: Int,name: String,channel: String,timestamp: String,c_d: Char,amount: Int)
  def parse2(row: String) :transaction2 = {
    val field = row.split(",")
    val account_id: Int = field(0).toInt
    val name: String = field(1)
    val channel: String = field(2)
    val timestamp: String = field(3).split("/")(2)
    val c_d: Char = field(4).toCharArray()(0)
    val amount: Int =  field(5).replace("$","").replaceAll(" ","").toInt
    
    transaction2(account_id,name,channel,timestamp,c_d,amount)
    
  }
  
}