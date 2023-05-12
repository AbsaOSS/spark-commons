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
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, NullType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.ErrorMessage
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitJustErrorValue, ErrorMessageSubmitOnColumn, ErrorMessageSubmitOnMoreColumns, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.errorhandling.types.ColumnOrValue.CoV
import za.co.absa.spark.commons.errorhandling.types.ErrorWhen
import za.co.absa.spark.commons.test.SparkTestBase

class ErrorMessageArrayTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private val nullString = Option.empty[String].orNull

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc"),
    (Some(0), "X")
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
      (Some(0), "X", List(
        ErrorMessage("Test error 1", 1, "This is a test error", Map("Col1" -> "0")),
        ErrorMessage("Test error 2", 2, "This is a test error", Map("Col2" -> "X")),
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
      (Some(0), "X", List.empty),
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
      (Some(0), "X", List.empty),
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

  test("Various error submits combined") {
    val errorMessageArray = ErrorMessageArray("MyErrCol")

    case class NullError(errColName: String) extends ErrorMessageSubmitOnColumn(
      CoV.withValue("Null Error"),
      CoV.withValue(1L),
      CoV.withValue("Field should not be null"),
      errColName)
    case class CorrelationError(lengthColName: String, textColumnName: String) extends  ErrorMessageSubmitOnMoreColumns(
      CoV.withValue("Columns not correlated"),
      CoV.withValue(2),
      CoV.withValue(s"Column '$textColumnName' doesn't have a length mentioned in column '$lengthColName'"),
      Set(lengthColName, textColumnName)
    )

    val expected: List[ResultDfRecordType] = List(
      (None, "", List(
        ErrorMessage("Null Error", 1, "Field should not be null", Map(col1Name -> nullString))
      )),
      (Some(0), "X", List(
        ErrorMessage(
          "Columns not correlated",
          2,
          s"Column '$col2Name' doesn't have a length mentioned in column '$col1Name'",
          Map(col1Name -> "0", col2Name -> "X"))
      )),
      (Some(1), "a", List.empty),
      (Some(2), "bb", List(
        ErrorMessage("ID is protected", 2, "The ID is too big", Map("" -> "2"))
      )),
      (Some(3), "ccc", List(
        ErrorMessage("Ugly row", 3, "I don't like this row", Map.empty)
      ))
    )

    val resultDf = errorMessageArray.putErrorsWithGrouping(srcDf)(Seq(
      ErrorWhen(col(col1Name).isNull, NullError(col1Name)),
      ErrorWhen(col(col1Name)  =!= length(col(col2Name)), CorrelationError(col1Name, col2Name)),
      ErrorWhen(col(col1Name) === 2, ErrorMessageSubmitJustErrorValue("ID is protected", 2, "The ID is too big", "2")),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitWithoutColumn("Ugly row", 3, "I don't like this row"))
    ))
    val result = resultDfToResult(resultDf)

    assert(result == expected)

  }

  test("test return errCol type schema") {
    val errorMessageArray = ErrorMessageArray("errCol")

    val expectedResults = StructType(Seq(
      StructField("errType", StringType, nullable = false),
      StructField("errCode", LongType, nullable = false),
      StructField("errMsg", StringType, nullable = false),
      StructField("errColsAndValues", MapType(StringType, StringType, valueContainsNull = true), nullable = false),
      StructField("additionInfo", StringType, nullable = true)
    ))

    val results = errorMessageArray.errorColumnType
    assert(results == expectedResults)
  }

}
