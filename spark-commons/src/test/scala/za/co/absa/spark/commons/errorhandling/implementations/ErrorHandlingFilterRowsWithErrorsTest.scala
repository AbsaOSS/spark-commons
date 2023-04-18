package za.co.absa.spark.commons.errorhandling.implementations
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class ErrorHandlingFilterRowsWithErrorsTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  val errorHandlingFilterRowsWithErrors = new ErrorHandlingFilterRowsWithErrorsTest()
  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val data = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)

  test("") {
    val errorMessageArray = ErrorMessageArray()

    val e1 = errorMessageArray.putErrorToColumn("Test error 1", 1, "This is a test error", Some(col1Name))
    val errorSubmitA = ErrorMessageSubmitOnColumn("Test error 2", 2, "This is a test error", col2Name)
    val e2 = errorMessageArray.putErrorToColumn(errorSubmitA)
    val errorSubmitB = ErrorMessageSubmitWithoutColumn("Test error 3", 3, "This is a test error")
    val e3 = errorMessageArray.putErrorToColumn(errorSubmitB)

    val result = ErrorHandlingFilterRowsWithErrors.aggregateErrorColumns(data)(e1, e2, e3)

    assert(result == false)
  }

}
