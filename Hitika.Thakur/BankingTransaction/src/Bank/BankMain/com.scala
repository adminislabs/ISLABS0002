package bank.com
import scala.util.matching.Regex
import org.apache.spark._
object com {
  def main(args: Array[String]): Unit = {
var sparkConf = new SparkConf().setMaster("local").setAppName("Transaction")
    var sc = new SparkContext(sparkConf)
sc.setLogLevel("ERROR")
    var path1rdd = sc.textFile("file:///home/hitika/Downloads/Transaction Sample data-1.csv")
    var path2rdd = sc.textFile("file:///home/hitika/Downloads/Transaction Sample data-2.csv")
    var filterRdd = path1rdd.filter(x => !x.contains("Account_id"))
    var filterrdd2 = path2rdd.filter(x => !x.contains("Account_id"))
    var parserdd = filterRdd.map(parse1)
    var parserdd2 = filterrdd2.map(parse2)

    var keyrdd = parserdd.map(x => (x.CDebit, x.Amount))
    var crdd = keyrdd.filter(x => (x._1) == 'C') // filtering 'C' from field Credit
    var drdd = keyrdd.filter(x => (x._1) == 'D') // filtering 'D' from field Credit
    var sumcrdd = crdd.map(x => (x._2)).sum()
    var sumdrdd = drdd.map(x => (x._2)).sum()
    var avcrdd = (sumcrdd / crdd.count()) // Average for 'C' from 1st file
    var avdrdd = sumdrdd / drdd.count() // Average for 'D' from 1st file

    var keyrdd2 = parserdd2.map(x => (x.CDebit, x.Amount))
    var crdd2 = keyrdd2.filter(x => (x._1) == 'C') // filtering 'C' from field Credit
    var drdd2 = keyrdd2.filter(x => (x._1) == 'D') // filtering 'D' from field Credit
    var sumcrdd2 = crdd2.map(x => (x._2)).sum()
    var sumdrdd2 = drdd2.map(x => (x._2)).sum()
    var avcrdd2 = (sumcrdd2 / crdd2.count())
    var avdrdd2 = sumdrdd2 / drdd2.count()
    var combocreditaverage = (avcrdd + avcrdd2) / 2 //  Average credit from both the files

    // println("Average Credit Transaction = " +combocreditaverage)

    var combodebitaverage = (avdrdd + avdrdd2) / 2 // Average debit from both the files

    // println("Average Debit Transaction = " +combodebitaverage)

    var yearrdd = parserdd.map(x => (x.Time, x.CDebit, x.Amount))
    var crdd3 = yearrdd.filter(x => (x._2) == 'C')
    var drdd3 = yearrdd.filter(x => (x._2) == 'D')
    var caserdd = crdd3.map { case (a, b, c) => (a, c) }
    var grouprdd = caserdd.groupByKey()
    var sumgrouprdd = grouprdd.map { case (k, v) => (k, v.sum) }
    var countgrouprdd = grouprdd.map { case (k, v) => (k, v.size) }
    var joingrouprdd = sumgrouprdd.join(countgrouprdd)
    var avgrouprdd = joingrouprdd.map { case (a, (b, c)) => (a, b / c) } //main1 credit 1st file

    var caserdd4 = drdd3.map { case (a, b, c) => (a, c) }
    var grouprdd4 = caserdd4.groupByKey()
    var sumgrouprdd4 = grouprdd4.map { case (k, v) => (k, v.sum) }
    var countgrouprdd4 = grouprdd4.map { case (k, v) => (k, v.size) }
    var joingrouprdd4 = sumgrouprdd4.join(countgrouprdd4)
    var avgrouprdd4 = joingrouprdd4.map { case (a, (b, c)) => (a, b / c) } // main debit 1st file

    // debit

    var debityearrdd = parserdd2.map(x => (x.Time, x.CDebit, x.Amount))
    var crdd5 = debityearrdd.filter(x => (x._2) == 'C')
    var drdd5 = debityearrdd.filter(x => (x._2) == 'D')
    var caserdd5 = crdd5.map { case (a, b, c) => (a, c) }
    var grouprdd5 = caserdd5.groupByKey()
    var sumgrouprdd5 = grouprdd5.map { case (k, v) => (k, v.sum) }
    var countgrouprdd5 = grouprdd5.map { case (k, v) => (k, v.size) }
    var joingrouprdd5 = sumgrouprdd5.join(countgrouprdd5)
    var avgrouprdd5 = joingrouprdd5.map { case (a, (b, c)) => (a, b / c) } //main1 credit 2nd file

    var caserdd6 = drdd5.map { case (a, b, c) => (a, c) }
    var grouprdd6 = caserdd6.groupByKey()
    var sumgrouprdd6 = grouprdd6.map { case (k, v) => (k, v.sum) }
    var countgrouprdd6 = grouprdd6.map { case (k, v) => (k, v.size) }
    var joingrouprdd6 = sumgrouprdd6.join(countgrouprdd6)
    var avgrouprdd6 = joingrouprdd6.map { case (a, (b, c)) => (a, b / c) } // main debit 2nd file

    var maincredit2avg = avgrouprdd.join(avgrouprdd5)
    var finalmaincredit2avg = maincredit2avg.map { case (a, (b, c)) => (a, (b + c) / 2) }
    //var show_credit_year = finalmaincredit2avg.map{case (a,b) => (f"Year = $a%5s" , f"Average Credit = $b%10s")} // 2 question credit both files

    var maincredit2avg3 = avgrouprdd4.join(avgrouprdd6)
    var finalmaincredit2avg2 = maincredit2avg3.map { case (a, (b, c)) => (a, (b + c) / 2) } // 2 question  debit both files
    //var show_debit_year = finalmaincredit2avg2.map{case (a,b) => (f"Year = $a%5s" , f"Average Debit = $b%10s")}

    var creditdata1 = parserdd.map(x => (x.Account_Id, x)).groupByKey().map { case (a, b) => (a, b.filter(_.CDebit == 'C').map(_.Amount)) }
    var sumc1 = creditdata1.map(x => (x._1, x._2.sum))// from first file
    var creditdata2 = parserdd2.map(x => (x.Account_Id, x)).groupByKey().map { case (a, b) => (a, b.filter(_.CDebit == 'C').map(_.Amount)) }
    var sumc2 = creditdata2.map(x => (x._1, x._2.sum))// from second file
    var aaa = sumc2.zip(sumc1).map { case ((a, b), (c, d)) => (a, b + d) }

    var debitdata1 = parserdd.map(x => (x.Account_Id, x)).groupByKey().map { case (a, b) => (a, b.filter(_.CDebit == 'D').map(_.Amount)) }
    var sumd1 = debitdata1.map(x => (x._1, x._2.sum))// from first file
    var debitdata2 = parserdd2.map(x => (x.Account_Id, x)).groupByKey().map { case (a, b) => (a, b.filter(_.CDebit == 'D').map(_.Amount)) }
    var sumd2 = debitdata2.map(x => (x._1, x._2.sum))// from second file
    var bbb = sumd2.zip(sumd1).map { case ((a, b), (c, d)) => (a, b + d) }

    var balance = aaa.zip(bbb).map { case ((a, b), (c, d)) => (a, b - d) }

    var balanceRdd = balance.map { case (a, b) => (f"Account_Id = $a%10s", f"Balance = $b%8s") }


    val maxKey2 = balance.max()(new Ordering[Tuple2[Int, Int]]() {
      override def compare(x: (Int, Int), y: (Int, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })
    println()
    println("Average Credit Transaction = " + combocreditaverage)
    println("Average Credit Transaction = " + combodebitaverage)
    println()
    var show_credit_year = finalmaincredit2avg.map{case (a,b) => (f"Year = $a%5s" , f"Average Credit = $b%10s")}.foreach(println)
println()																																																																																																																																																																			
    var show_debit_year = finalmaincredit2avg2.map{case (a,b) => (f"Year = $a%5s" , f"Average Debit = $b%11s")}.foreach(println)
println()
    println("Account Id Whose balance is maximum along with Balance = " + maxKey2)
println()
    sc.stop()

  }
  case class bank1(Account_Id: Int, Name: String, Time: Int, CDebit: Char, Amount: Int)

  def parse1(row: String): bank1 = {
    var fields = row.split(",")
    var Account_Id: Int = fields(0).toInt
    var Name: String = fields(1).replace('"', ' ')
    var Time: Int = fields(2).substring(fields(2).length() - 4, fields(2).length()).toInt
    var CDebit: Char = fields(3).toCharArray()(0)
    var Amount: Int = fields(4).replace("$", "").replace('"', ' ').replace(" ", "").trim().toInt

    bank1(Account_Id, Name, Time, CDebit, Amount)
  }

  case class bank2(Account_Id: Int, Name: String, Channel: String, Time: Int, CDebit: Char, Amount: Int)
  def parse2(row: String): bank2 = {
    var fields = row.split(",")
    var Account_Id: Int = fields(0).toInt
    var Name: String = fields(1)
    var Channel: String = fields(2)
    var Time: Int = fields(3).substring(fields(3).length() - 4, fields(3).length()).toInt
    var CDebit: Char = fields(4).toCharArray()(0)
    var Amount: Int = fields(5).replace("$", "").toInt

    bank2(Account_Id, Name, Channel, Time, CDebit, Amount)
  }
}
