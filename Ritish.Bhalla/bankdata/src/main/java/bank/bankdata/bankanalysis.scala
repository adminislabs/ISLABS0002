package bank.bankdata
import org.apache.spark._
object bankanalysis{
def main(arg: Array[String]): Unit = {
var sparkConf = new SparkConf()
var sc = new SparkContext(sparkConf)
var firstRdd = sc.textFile("file:///home/ritish/Downloads/Transaction Sample data-1.csv")
var secondRdd = sc.textFile("file:///home/ritish/Downloads/Transaction Sample data-2.csv")
var filterRdd = firstRdd.filter(x => !x.contains("Account_id"))
var filterRdd2 = secondRdd.filter(x => !x.contains("Account_id"))
var bankdata = filterRdd.map(parse)
var bankdata2 = filterRdd2.map(parse2)

var filterCredit = bankdata.filter(x => x.card == 'C')
var pairRddCredit = filterCredit.map(x => (x.card,x.amount)).groupByKey().collect()
var finalCreditsize  = pairRddCredit.map{case (key,value) => (key,value.size)} 
var finalCreditsum = pairRddCredit.map{case (key,value) => (key,value.sum)}
var zipRddCredit = finalCreditsize.zip(finalCreditsum).map{case ((a,b),(c,d)) => (a,d/b)}

var filterCredit2 = bankdata2.filter(x => x.card == 'C')
var pairRddCredit2 = filterCredit2.map(x => (x.card,x.amount)).groupByKey().collect()
var finalCreditsize2  = pairRddCredit2.map{case (key,value) => (key,value.size)} 
var finalCreditsum2 = pairRddCredit2.map{case (key,value) => (key,value.sum)}
var zipRddCredit2 = finalCreditsize2.zip(finalCreditsum2).map{case ((a,b),(c,d)) => (a,d/b)}
var zippedCredit = zipRddCredit.zip(zipRddCredit2).map{case ((a,b),(c,d)) => (a,(b+d)/2)}
var averageof_CreditCard = zippedCredit.map{case (a,b) => (f"Average Transaction Amount from Credit = $b%.2f")}

var filterDebit = bankdata.filter(x => x.card == 'D')
var pairRddDebit = filterDebit.map(x => (x.card,x.amount)).groupByKey().collect()
var finalDebitsize  = pairRddDebit.map{case (key,value) => (key,value.size)} 
var finalDebitsum = pairRddDebit.map{case (key,value) => (key,value.sum)}
var zipRddDebit = finalDebitsize.zip(finalDebitsum).map{case ((a,b),(c,d)) => (a,d/b)}

var filterDebit2 = bankdata2.filter(x => x.card == 'D')
var pairRddDebit2 = filterDebit2.map(x => (x.card,x.amount)).groupByKey().collect()
var finalDebitsize2  = pairRddDebit2.map{case (key,value) => (key,value.size)} 
var finalDebitsum2 = pairRddDebit2.map{case (key,value) => (key,value.sum)}
var zipRddDebit2 = finalDebitsize2.zip(finalDebitsum2).map{case ((a,b),(c,d)) => (a,d/b)}
var zippedDebit = zipRddDebit.zip(zipRddDebit2).map{case ((a,b),(c,d)) => (a,(b+d)/2)}
var averageof_DebitCard = zippedDebit.map{case (a,b) => (f"Average Transaction Amount from Debit = $b%.2f")}

var groupYear = filterCredit.map(x => (x.year,x.amount)).groupByKey().collect()
var finalyearsize  = groupYear.map{case (key,value) => (key,value.size)} 
var finalyearsum = groupYear.map{case (key,value) => (key,value.sum)}
var zip1 = finalyearsize.zip(finalyearsum).map{case ((a,b),(c,d)) =>(a,d/b)}
var groupYear2 =filterCredit2.map(x => (x.year,x.amount)).groupByKey().collect()
var finalyearsize2  = groupYear2.map{case (key,value) => (key,value.size)} 
var finalyearsum2 = groupYear2.map{case (key,value) => (key,value.sum)}
var zip2 = finalyearsize2.zip(finalyearsum2).map{case ((a,b),(c,d)) =>(a,d/b)}
var yearCredit = zip1.zip(zip2).map{case ((a,b),(c,d)) => (a,(b+d)/2)}
var averageof_allyear = yearCredit.map{case (a,b) =>(f"Year = $a%5s",f"Average Transaction Amount from Credit =$b%.2f")}

var groupYear_debit = filterDebit.map(x => (x.year,x.amount)).groupByKey().collect()
var finalyearsize_debit  = groupYear_debit.map{case (key,value) => (key,value.size)} 
var finalyearsum_debit = groupYear_debit.map{case (key,value) => (key,value.sum)}
var zip1_debit = finalyearsize_debit.zip(finalyearsum_debit).map{case ((a,b),(c,d)) =>(a,d/b)}
var groupYear2_debit =filterDebit2.map(x => (x.year,x.amount)).groupByKey().collect()
var finalyearsize2_debit  = groupYear2_debit.map{case (key,value) => (key,value.size)} 
var finalyearsum2_debit = groupYear2_debit.map{case (key,value) => (key,value.sum)}
var zip2_debit = finalyearsize2_debit.zip(finalyearsum2_debit).map{case ((a,b),(c,d)) =>(a,d/b)}
var yearCredit_debit = zip1_debit.zip(zip2_debit).map{case ((a,b),(c,d)) => (a,(b+d)/2)}
var averageof_allyear_debit = yearCredit_debit.map{case (a,b) =>(f"Year = $a%5s",f"Average Transaction Amount from Debit =$b%.2f")}

var credit1rdd = bankdata.map(x => (x.account_id,x)).groupByKey().map{case (a,b) => (a,b.filter(_.card == 'C').map(_.amount))}
var sum_C1 = credit1rdd.map(x => (x._1,x._2.sum))
var credit11rdd = bankdata2.map(x => (x.account_id,x)).groupByKey().map{case (a,b) => (a,b.filter(_.card == 'C').map(_.amount))}
var sum_C2 = credit11rdd.map(x => (x._1,x._2.sum))
var aaa = sum_C1.zip(sum_C2).map{case ((a,b),(c,d)) => (a,b+d)}

var debit2rdd = bankdata.map(x => (x.account_id,x)).groupByKey().map{case (a,b) => (a,b.filter(_.card == 'D').map(_.amount))}
var sum_D1 = debit2rdd.map(x => (x._1,x._2.sum))
var debit22rdd = bankdata2.map(x => (x.account_id,x)).groupByKey().map{case (a,b) => (a,b.filter(_.card == 'D').map(_.amount))}
var sum_D2 = debit22rdd.map(x => (x._1,x._2.sum))
var bbb = sum_D1.zip(sum_D2).map{case ((a,b),(c,d)) => (a,b+d)}

var balance = aaa.zip(bbb).map{case ((a,b),(c,d)) => (a, b-d)}
var balanceRdd = balance.map{case (a,b) => (f"Account_Id = $a%10s", f"Balance = $b%8s")}

var maxKey2 = balance.max()(new Ordering[Tuple2[Int, Float]]() {
  override def compare(x: (Int, Float), y: (Int, Float)): Int = 
      Ordering[Float].compare(x._2, y._2)
})
}
case class Bank(account_id: Int, name: String, year: String, card: Char, amount: Float) 
def parse(row: String): Bank= {
val fields = row.split(",")
var account_id: Int = fields(0).toInt
var name: String = fields(1)
var year: String = fields(2).subSequence(6,10).toString()
var card: Char = fields(3).toCharArray()(0)
var amount: Float = fields(4).substring(1).replaceAll("\\s", "").toFloat
Bank(account_id,name,year,card,amount)
}
case class Bank2(account_id: Int, name: String, year: String, card: Char, amount: Float) extends Serializable {}
def parse2(row: String): Bank2= {
val fields = row.split(",")
var account_id: Int = fields(0).toInt
var name: String = fields(1)
var year: String = fields(3).subSequence(6,10).toString()
var card: Char = fields(4).toCharArray()(0)
var amount: Float = fields(5).substring(1).replaceAll("\\s", "").toFloat
Bank2(account_id,name,year,card,amount)
}
}
