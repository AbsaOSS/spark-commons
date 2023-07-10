/*
 * Copyright 2021 ABSA Group Limited
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

import org.apache.spark.sql.functions.{col, length}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.errorhandling.types.{ErrorColumn, ErrorWhen}
import za.co.absa.spark.commons.test.SparkTestBase

class ErrorHandlingIgnoringErrorsTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val errColName = "Null_col"
  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)
  private val emptyDf = spark.emptyDataFrame

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

  test("putError and putErrors should return the original dataFrame on group of errors applied") {

    val midDf = ErrorHandlingIgnoringErrors.putError(srcDf)(col(col1Name) > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", col1Name))

    val resultDf = ErrorHandlingIgnoringErrors.putErrorsWithGrouping(midDf)(Seq(
      ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("WrongLine", 0, "This line is wrong")),
      ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 10, "The text in the field is too long", col2Name))
    ))

    assert(resultDf == srcDf)
  }

  test("errorColumnType should return a BooleanType") {
    val errorColumn: ErrorColumn = ErrorHandlingIgnoringErrors.createErrorAsColumn(
      "Test error 1", 1, "This is a test error", Some(errColName))

    val testDf = emptyDf.withColumn(errColName, errorColumn.column)
    val expectedType = testDf.col(errColName).expr.dataType
    val expectedValue = testDf.schema.fields
    val actualType = ErrorHandlingIgnoringErrors.errorColumnType

    assert(actualType.defaultSize == expectedValue.length)
    assert(actualType == expectedType)
  }

  test("dataFrameColumnType should return None since no column is added during the aggregation") {
    val errorColumn: ErrorColumn = ErrorHandlingIgnoringErrors.createErrorAsColumn(
      "1st error", 0, "This is an error", Some(errColName)
    )
    val testDf = emptyDf

    val expectedAfterAgg = ErrorHandlingIgnoringErrors.applyErrorColumnsToDataFrame(testDf)(errorColumn)
    val expectedTypeAfterAgg = expectedAfterAgg.schema.fields.headOption
    val actualType = ErrorHandlingIgnoringErrors.dataFrameColumnType

    assert(actualType == expectedTypeAfterAgg)
  }

}
