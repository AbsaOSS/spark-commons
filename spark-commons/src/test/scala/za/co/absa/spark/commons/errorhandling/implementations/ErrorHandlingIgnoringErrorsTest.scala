package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.test.SparkTestBase

class ErrorHandlingIgnoringErrorsTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)

  test("aggregateErrorColumns should return the original dataFrame after error aggregation") {
    val e1 = ErrorHandlingIgnoringErrors.createErrorAsColumn("Test error 1", 1, "This is a test error", Some(col1Name))
    val errorSubmitA = ErrorMessageSubmitOnColumn("Test error 2", 2, "This is a test error", col2Name)
    val e2 = ErrorHandlingIgnoringErrors.createErrorAsColumn(errorSubmitA)
    val errorSubmitB = ErrorMessageSubmitWithoutColumn("Test error 3", 3, "This is a test error")
    val e3 = ErrorHandlingIgnoringErrors.createErrorAsColumn(errorSubmitB)

    val resultsDF = ErrorHandlingIgnoringErrors.applyErrorColumnsToDataFrame(srcDf)(e1, e2, e3)

    assert(resultsDF.count() == srcDf.count())
    assert(resultsDF == srcDf)
  }


}
