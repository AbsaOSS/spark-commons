/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.types.ErrorWhen
import za.co.absa.spark.commons.test.SparkTestBase
import org.apache.spark.sql.functions.{col, length}
import za.co.absa.spark.commons.errorhandling.ErrorMessage
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}


class ErrorMessageArrayTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private val nullString = Option.empty[String].orNull

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)

  private type ResultDfRecordType = (Option[Integer], String, List[ErrorMessage])

  private def resultDfToResult(resultDf: DataFrame): List[ResultDfRecordType] = {
    resultDf.as[ResultDfRecordType].collect().sortBy(_._1).toList
  }

  test("Collect columns and aggregate them explicitly") {
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

    val errorMessageArray = ErrorMessageArray()

    val e1 = errorMessageArray.putErrorToColumn("Test error 1", 1, "This is a test error", Some(col1Name))
    val errorSubmitA = ErrorMessageSubmitOnColumn("Test error 2", 2, "This is a test error", col2Name)
    val e2 = errorMessageArray.putErrorToColumn(errorSubmitA)
    val errorSubmitB = ErrorMessageSubmitWithoutColumn("Test error 3", 3, "This is a test error")
    val e3 = errorMessageArray.putErrorToColumn(errorSubmitB)

    val resultDf = errorMessageArray.aggregateErrorColumns(srcDf)(e1, e2, e3)
    val result = resultDfToResult(resultDf)

    assert(result == expected)
  }

  test("putErrors groups conditions by source column"){
    val errorMessageArray = ErrorMessageArray()
    val expected: List[ResultDfRecordType] = List(
      (None, "", List(
        ErrorMessage("WrongLine", 0, "This line is wrong", Map.empty)
      )),
      (Some(1), "a", List.empty),
      (Some(2), "bb", List(
        ErrorMessage("ValueStillTooBig", 2, "The value of the field is too big", Map("Col1" -> "2"))
      )),
      (Some(3), "ccc", List(
        ErrorMessage("ValueTooBig", 1, "The value of the field is too big", Map("Col1" -> "3")),
        ErrorMessage("String too long", 10, "The text in the field is too long", Map("Col2" -> "ccc"))
      ))
    )

    val resultDf = errorMessageArray.putErrorsWithGrouping(srcDf)(Seq(
      ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("WrongLine", 0, "This line is wrong")),
      ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)),
      ErrorWhen(col(col1Name) > 1, ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", col1Name)),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 10, "The text in the field is too long", col2Name))
    ))
    val result = resultDfToResult(resultDf)

    assert(result == expected)
  }

  test("putError and putErrors does not group by together"){
    val errorMessageArray = ErrorMessageArray()
    val expected: List[ResultDfRecordType] = List(
      (None, "", List(
        ErrorMessage("WrongLine", 0, "This line is wrong", Map.empty)
      )),
      (Some(1), "a", List.empty),
      (Some(2), "bb", List(
        ErrorMessage("ValueStillTooBig", 2, "The value of the field is too big", Map("Col1" -> "2"))
      )),
      (Some(3), "ccc", List(
        ErrorMessage("ValueStillTooBig", 2, "The value of the field is too big", Map("Col1" -> "3")),
        ErrorMessage("ValueTooBig", 1, "The value of the field is too big", Map("Col1" -> "3")),
        ErrorMessage("String too long", 10, "The text in the field is too long", Map("Col2" -> "ccc"))
      ))
    )

    val midDf = errorMessageArray.putError(srcDf)(col(col1Name) > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", col1Name))

    val resultDf = errorMessageArray.putErrorsWithGrouping(midDf)(Seq(
      ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("WrongLine", 0, "This line is wrong")),
      ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 10, "The text in the field is too long", col2Name))
    ))
    val result = resultDfToResult(resultDf)

    assert(result == expected)
  }
}
