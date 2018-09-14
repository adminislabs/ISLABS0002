import org.apache.spark._

package object bank {
  def main(args: Array[String]) = {
    var sparkConf = new SparkConf()
    var sc = new SparkContext(sparkConf)
    var firstRdd = sc.textFile("file:///home/kiran/Downloads/Transaction Sample data-1.csv")
    var secondRdd = sc.textFile("file:///home/kiran/Downloads/Transaction Sample data-2.csv")
    var filterRdd1 = firstRdd.filter(x => !x.contains("Name"))
    var filterRdd2 = secondRdd.filter(x => !x.contains("Name"))
    var newData1 = filterRdd1.map(parse)
    var newData2 = filterRdd2.map(parse2)

    var x1 = newData1.filter(x => x.credit_debit == "C")
    var y1 = x1.map(a => a.amount)
    var sum1 = y1.reduce(_ + _)
    var average_credit1 = (sum1 / x1.count) //1st file credit

    var x2 = newData1.filter(x => x.credit_debit == "D")
    var y2 = x2.map(a => a.amount)
    var sum2 = y2.reduce(_ + _)
    var average_debit1 = (sum2 / x2.count) //1st file debit

    var x3 = newData2.filter(x => x.credit_debit == "C")
    var y3 = x3.map(a => a.amount)
    var sum3 = y3.reduce(_ + _)
    var average_credit2 = (sum3 / x3.count) //2nd file credit

    var x4 = newData2.filter(x => x.credit_debit == "D")
    var y4 = x4.map(a => a.amount)
    var sum4 = y4.reduce(_ + _)
    var average_debit2 = (sum4 / x4.count) //2nd file debit

    var average_credit = ((average_credit1 + average_credit2) / 2) // average credit
    var average_debit = ((average_debit1 + average_debit2) / 2) //  average debit

    var a1 = x1.map(x => ((x.time_stamp), x))
    var b1 = a1.mapValues(x => x.amount).reduceByKey(_ + _)
    var c1 = x1.map(x => ((x.time_stamp), 1)).reduceByKey(_ + _)
    var avgcreditforeachyear1 = b1.join(c1).map { case (a, (b, c)) => (a, b / c) } //1st file credit for each year

    var a2 = x2.map(x => ((x.time_stamp), x))
    var b2 = a2.mapValues(x => x.amount).reduceByKey(_ + _)
    var c2 = x2.map(x => ((x.time_stamp), 1)).reduceByKey(_ + _)
    var avgdebitforeachyear1 = b2.join(c2).map { case (a, (b, c)) => (a, b / c) } // 1st file debit for each year

    var a3 = x3.map(x => ((x.time_stamp), x))
    var b3 = a3.mapValues(x => x.amount).reduceByKey(_ + _)
    var c3 = x3.map(x => ((x.time_stamp), 1)).reduceByKey(_ + _)
    var avgcreditforeachyear2 = b3.join(c3).map { case (a, (b, c)) => (a, b / c) } //2nd file credit for each year

    var a4 = x4.map(x => ((x.time_stamp), x))
    var b4 = a4.mapValues(x => x.amount).reduceByKey(_ + _)
    var c4 = x4.map(x => ((x.time_stamp), 1)).reduceByKey(_ + _)
    var avgdebitforeachyear2 = b4.join(c4).map { case (a, (b, c)) => (a, b / c) } //2nd file debit for each year

    var average_credit_for_each_year = avgcreditforeachyear1.join(avgcreditforeachyear2).map { case (a, (b, c)) => (a, ((b + c) / 2)) } // average credit year wise
    var average_debit_for_each_year = avgdebitforeachyear1.join(avgdebitforeachyear2).map { case (a, (b, c)) => (a, ((b + c) / 2)) } // average debit year wise

    val result1 = y1.max()
    val newrdd1 = x1.filter(x => x.amount == result1)
    val account_with_maxcredit1 = newrdd1.map(x => x.account_id)
    val result2 = y2.max()
    val newrdd2 = x2.filter(x => x.amount == result2)
    val account_with_maxdebit2 = newrdd2.map(x => x.account_id)
    val result3 = y3.max()
    val newrdd3 = x3.filter(x => x.amount == result3)
    val account_with_maxcredit3 = newrdd3.map(x => x.account_id)
    val result4 = y4.max()
    val newrdd4 = x4.filter(x => x.amount == result4)
    val account_with_maxdebit4 = newrdd4.map(x => x.account_id)

    var pairrdd1 = newData1.map(x => ((x.account_id), x)).groupByKey().map(x => (x._1, x._2.filter(_.credit_debit == "C"))).map(x => (x._1, x._2.map(_.amount))).map(x => (x._1, x._2.sum))

    var pairrdd2 = newData1.map(x => ((x.account_id), x)).groupByKey().map(x => (x._1, x._2.filter(_.credit_debit == "D"))).map(x => (x._1, x._2.map(_.amount))).map(x => (x._1, x._2.sum))

    var pairrdd3 = newData2.map(x => ((x.account_id), x)).groupByKey().map(x => (x._1, x._2.filter(_.credit_debit == "C"))).map(x => (x._1, x._2.map(_.amount))).map(x => (x._1, x._2.sum))

    var pairrdd4 = newData2.map(x => ((x.account_id), x)).groupByKey().map(x => (x._1, x._2.filter(_.credit_debit == "D"))).map(x => (x._1, x._2.map(_.amount))).map(x => (x._1, x._2.sum))

    var m1 = pairrdd1.zip(pairrdd3).map { case ((a, b), (c, d)) => (a, b + d) }
    var m2 = pairrdd2.zip(pairrdd4).map { case ((a, b), (c, d)) => (a, b + d) }

    var kk = m1.zip(m2).map { case ((a, b), (c, d)) => (a, b - d) }.groupByKey

    var filterdd = kk.map(x => x._2)
    var result = filterdd.max()

    var account_with_max_balance = kk.filter(x => x._2 == result).map { case (a, b) => (f"Account=$a " + f"Balance=$b") }.foreach(println)

  }

  case class bank1(account_id: Int, name: String, time_stamp: String, credit_debit: String, amount: Int)
  def parse(row: String): bank1 = {
    val fields = row.split(",")
    val account_id: Int = fields(0).toInt
    val name: String = fields(1)
    val time_stamp: String = fields(2).split("/")(2)
    val credit_debit: String = fields(3)
    fields(4) = fields(4).split(" ")(0)
    val amount: Int = fields(4).replace("$", "").toInt
    bank1(account_id, name, time_stamp, credit_debit, amount)
  }

  case class bank2(account_id: Int, name: String, channel: String, time_stamp: String, credit_debit: String, amount: Int)
  def parse2(row: String): bank2 = {

    val fields = row.split(",")
    val account_id: Int = fields(0).toInt
    val name: String = fields(1)
    val channel: String = fields(2)
    val time_stamp: String = fields(3).split("/")(2)
    val credit_debit: String = fields(4)
    fields(4) = fields(4).split(" ")(0)
    val amount: Int = fields(5).replace("$", "").toInt
    bank2(account_id, name, channel, time_stamp, credit_debit, amount)

  }

}