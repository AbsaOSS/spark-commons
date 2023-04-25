/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.commons.errorhandling.implementations
import org.apache.hadoop.shaded.com.sun.jersey.spi.inject.Errors.ErrorMessage
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.ErrorMessage
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}

class ErrorHandlingFilterRowsWithErrorsTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private val nullString = Option.empty[String].orNull

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val data = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)

  private type ResultDfRecordType = (Option[Integer], String, List[ErrorMessage])

  private def resultDfToResult(resultDf: DataFrame): List[ResultDfRecordType] = {
    resultDf.as[ResultDfRecordType].collect().sortBy(_._1).toList
  }

  test("aggregateErrorColumns\" should \"return a DataFrame with the specified columns aggregated\"") {
    val expected: List[ResultDfRecordType] = List(
      (None, "", List(
        ErrorMessage("Test error 1", 1, "This is a test error", Map("Col1" -> nullString)),
        ErrorMessage("Test error 2", 2, "This is a test error", Map("Col2" -> "")),
        ErrorMessage("Test error 3", 3, "This is a test error", Map.empty)
      )),
      (Some(1), "a", List(
        ErrorMessage("Test error 1", 1, "This is a test error", Map("Col1" -> "1")),
        ErrorMessage("Test error 2", 2, "This is a test error", Map("Col2" -> "a")),
        ErrorMessage("Test error 3", 3, "This is a test error", Map.empty)
      )),
      (Some(2), "bb", List(
        ErrorMessage("Test error 1", 1, "This is a test error", Map("Col1" -> "2")),
        ErrorMessage("Test error 2", 2, "This is a test error", Map("Col2" -> "bb")),
        ErrorMessage("Test error 3", 3, "This is a test error", Map.empty)
      )),
      (Some(3), "ccc", List(
        ErrorMessage("Test error 1", 1, "This is a test error", Map("Col1" -> "3")),
        ErrorMessage("Test error 2", 2, "This is a test error", Map("Col2" -> "ccc")),
        ErrorMessage("Test error 3", 3, "This is a test error", Map.empty)
      ))
    )

    val e1 = ErrorHandlingFilterRowsWithErrors.putErrorToColumn("Test error 1", 1, "This is a test error", Some(col1Name))
    val errorSubmitA = ErrorMessageSubmitOnColumn("Test error 2", 2, "This is a test error", col2Name)
    val e2 = ErrorHandlingFilterRowsWithErrors.putErrorToColumn(errorSubmitA)
    val errorSubmitB = ErrorMessageSubmitWithoutColumn("Test error 3", 3, "This is a test error")
    val e3 = ErrorHandlingFilterRowsWithErrors.putErrorToColumn(errorSubmitB)

    val dfSchema = Seq("Col1","Col2").toList

    val results = ErrorHandlingFilterRowsWithErrors.aggregateErrorColumns(data)(e1, e2, e3)
    results.printSchema()
    results.show(false)
//    assert(results.schema == dfSchema)
  }

}
