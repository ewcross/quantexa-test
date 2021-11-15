import scala.io.Source

object Solution {
  def main(args: Array[String]) = {

    //Define a case class Transaction which represents a transaction
    case class Transaction(
        transactionId: String,
        accountId: String,
        transactionDay: Int,
        category: String,
        transactionAmount: Double
    )

    // definitions of pure methods

    def roundDouble(double: Double, dp: Int): Double =
      BigDecimal(double).setScale(dp, BigDecimal.RoundingMode.HALF_UP).toDouble

    def calcTotalPerDay(transList: List[Transaction]): Seq[(Int, Double)] =
      transList
        .groupBy(_.transactionDay)
        .map { case (key, transList) =>
          key ->
            roundDouble(
              transList.foldLeft(0.0)(_ + _.transactionAmount),
              2
            )
        }
        .toSeq
        .sortBy(_._1)

    def calcAvPerCategory(
        inputMap: Map[String, List[Transaction]]
    ): Map[String, Double] =
      inputMap.map { case (key, transList) =>
        key ->
          roundDouble(
            transList.foldLeft(0.0)(_ + _.transactionAmount) / transList.length,
            2
          )
      }

    /*
     * Calculate average transaction amount for each category
     * per account type
     */
    def calcAvPerAccPerType(
        transList: List[Transaction]
    ): Seq[(String, Map[String, Double])] =
      transList
        .groupBy(_.accountId)
        .map { case (accId, transList) =>
          accId -> calcAvPerCategory(transList.groupBy(_.category))
        }
        .toSeq
        .sortBy(_._1.substring(1).toInt)

    def getMaxAmount(max: Double, trans: Transaction): Double =
      if (trans.transactionAmount > max) trans.transactionAmount else max

    def calcListAv(transList: List[Transaction]): Double =
      roundDouble(
        transList.foldLeft(0.0)(_ + _.transactionAmount) / transList.length,
        2
      )

    /*
     * Group transactions by category and calculate total for each
     */
    def getCategoryTotalsMap(
        transList: List[Transaction]
    ): Map[String, Double] =
      transList
        .groupBy(_.category)
        .map { case (cat, tList) =>
          cat -> roundDouble(
            tList.foldLeft(0.0)(_ + _.transactionAmount),
            2
          )
        }

    /*
     * Group transactions by accountId and calculate required
     * data per account
     * All transaction category totals are calculated ("AA" -> "FF"),
     * to be filtered in data output function
     */
    def calcWindowData(
        transList: List[Transaction]
    ): Seq[((String), (Double, Double, Map[String, Double]))] =
      transList
        .groupBy(_.accountId)
        .map { case (accId, tList) =>
          accId ->
            (
              tList.foldLeft(0.0)(getMaxAmount),
              calcListAv(tList),
              getCategoryTotalsMap(tList)
            )
        }
        .toSeq
        .sortBy(_._1.substring(1).toInt)

    /*
     * Recursively moves through transList by successive day of the month,
     * calculating window data for 'winSize' days preceeding currDay.
     *
     * Note: starting on a day less than winSize will give smaller data windows
     * until currDay = winSize + 1
     */
    def calcRollingStats(
        currDay: Int,
        finalDay: Int,
        winSize: Int,
        transList: List[Transaction]
    ): List[(Int, Seq[((String), (Double, Double, Map[String, Double]))])] = {
      // empty list returned if bad inputs
      if (currDay < 1 || currDay > finalDay || winSize < 1) {
        List()
      } else {
        // generate sub-list with days in current window
        val winStart = currDay - winSize
        val window = transList.filter(trans =>
          trans.transactionDay >= winStart && trans.transactionDay < currDay
        )

        if (currDay == finalDay) {
          List((currDay, calcWindowData(window)))
        } else {
          List((currDay, calcWindowData(window))) ++ calcRollingStats(
            currDay + 1,
            finalDay,
            winSize,
            transList
          )
        }
      }
    }

    // side effect methods for data input/output

    def importTransList(fileName: String): List[Transaction] = {
      // get lines of the CSV file (dropping the first to remove the header)
      val transactionslines = Source
        .fromFile(fileName)
        .getLines()
        .drop(1)

      //split each line up by commas and construct Transactions
      transactionslines.map { line =>
        val split = line.split(',')
        Transaction(
          split(0),
          split(1),
          split(2).toInt,
          split(3),
          split(4).toDouble
        )
      }.toList
    }

    def printSolutionOne(result: Seq[(Int, Double)]): Unit = {
      println("Day,Total")
      result.foreach { case (day, total) => println(s"${day},${total}") }
    }

    def printSolutionTwo(result: Seq[(String, Map[String, Double])]): Unit = {
      println(
        "Account ID,AA Av Value,BB Av Value,CC Av Value,DD Av Value,EE Av Value,FF Av Value,GG Av Value"
      )
      result.foreach {
        case (accId, cats) => {
          println(
            s"$accId," +
              s"${cats.getOrElse("AA", 0.0)}," +
              s"${cats.getOrElse("BB", 0.0)}," +
              s"${cats.getOrElse("CC", 0.0)}," +
              s"${cats.getOrElse("DD", 0.0)}," +
              s"${cats.getOrElse("EE", 0.0)}," +
              s"${cats.getOrElse("FF", 0.0)}," +
              s"${cats.getOrElse("GG", 0.0)}"
          )
        }
      }
    }

    def printSolutionThree(
        result: List[
          (Int, Seq[((String), (Double, Double, Map[String, Double]))])
        ]
    ): Unit = {
      println(
        "Day, Account ID, Maximum, Average, AA Total Value, CC Total Value, FF Total Value"
      )
      result.foreach {
        case (day, accountMap) => {
          accountMap.foreach { case (accId, data) =>
            println(
              s"$day,$accId,${data._1},${data._2},${data._3.getOrElse("AA", 0.0)},${data._3
                .getOrElse("CC", 0.0)},${data._3.getOrElse("FF", 0.0)}"
            )
          }
        }
      }
    }

    // data import

    //The full path to the file to import
    val fileName = s"${System.getProperty("user.dir")}/transactions.txt"
    val transactions = importTransList(fileName)

    // calculations

    val resOne = calcTotalPerDay(transactions)
    val resTwo = calcAvPerAccPerType(transactions)
    val resThree = calcRollingStats(6, 29, 5, transactions)

    // results output

    println("=================================")
    println("==Solution 1==")
    println("=================================")

    printSolutionOne(resOne)

    println("=================================")
    println("==Solution 2==")
    println("=================================")

    printSolutionTwo(resTwo)

    println("=================================")
    println("==Solution 3==")
    println("=================================")

    printSolutionThree(resThree)
  }
}
