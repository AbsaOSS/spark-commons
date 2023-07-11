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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.{DataFrameErrorHandlingImplicit, ErrorHandler}
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.errorhandling.types.{ErrorColumn, ErrorWhen}

class ErrorHandlerFilteringErrorRowsTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._
  import DataFrameErrorHandlingImplicit._

  implicit private val errorHandling: ErrorHandler = ErrorHandlerFilteringErrorRows

  private val col1Name = "Col1"
  private val col2Name = "Col2"
  private val errColName = "error_column"
  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)
  private val emptyDf = spark.emptyDataFrame

  private type ResultDfRecordType = (Option[Integer], String)
  private def resultDfToResult(resultDf: DataFrame): List[ResultDfRecordType] = {
    resultDf.as[ResultDfRecordType].collect().sortBy(_._1).toList
  }

  test("aggregateErrorColumns should return an empty list after error aggregation") {
    val expectedResults: List[ResultDfRecordType] = List()

    val e1 = srcDf.createErrorAsColumn("Test error 1", 1, "This is a test error", Some(col1Name))
    val errorSubmitA = ErrorMessageSubmitOnColumn("Test error 2", 2, "This is a test error", col2Name)
    val e2 = srcDf.createErrorAsColumn(errorSubmitA)
    val errorSubmitB = ErrorMessageSubmitWithoutColumn("Test error 3", 3, "This is a test error")
    val e3 = srcDf.createErrorAsColumn(errorSubmitB)

    val resultsDF = srcDf.applyErrorColumnsToDataFrame(e1, e2, e3)
    val results = resultDfToResult(resultsDF)

    assert(results.length == expectedResults.length)
    assert(results == expectedResults)
  }

  test("aggregateErrorColumns should return  records whose don't have errors") {
    val expectedResults: List[ResultDfRecordType] = List(
      (Some(1),"a"),
      (Some(2),"bb")
    )

    val er1 = ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("Test error 3", 0, "This is a test error"))
    val er2 = ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the column is too big", col1Name))
    val er3 = ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 5, "The text in the field is too long", col2Name))

    // The putErrorsWithGrouping calls the doAggregationErrorColumns method implemented in ErrorHandlerFilteringErrorRows object
    val resultsDf = srcDf.putErrorsWithGrouping(Seq(er1, er2, er3))
    val results = resultDfToResult(resultsDf)

    assert(results == expectedResults)
  }

  test("putError and putErrors does not group by together") {
    val expected: List[ResultDfRecordType] = List((Some(1),"a"))

    val midDf = srcDf.putError(col(col1Name) > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", col1Name))

    val resultDf = midDf.putErrorsWithGrouping(Seq(
      ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("WrongLine", 0, "This line is wrong")),
      ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 10, "The text in the field is too long", col2Name))
    ))
    val result = resultDfToResult(resultDf)

    assert(result == expected)
  }

  test("errorColumnType should return a BooleanType") {
    val errorColumn: ErrorColumn = srcDf.createErrorAsColumn(
      "Test error 1", 1, "This is a test error", Some(errColName))

    val testDf = emptyDf.withColumn(errColName, errorColumn.column)
    val expectedType = testDf.col(errColName).expr.dataType
    val expectedValue = testDf.schema.fields

    val actualType = ErrorHandlerFilteringErrorRows.errorColumnType

    assert(actualType.defaultSize == expectedValue.length)
    assert(actualType == expectedType)
  }

  test("dataFrameColumnType should return None since no column is added during the aggregation") {
    val errorColumn: ErrorColumn = srcDf.createErrorAsColumn(
      "1st error", 0, "This is an error", Some(errColName)
    )

    val testDf = emptyDf

    val expectedAfterAgg = testDf.applyErrorColumnsToDataFrame(errorColumn)
    val expectedTypeAfterAgg = expectedAfterAgg.schema.fields.headOption

    val actualType = ErrorHandlerFilteringErrorRows.dataFrameColumnType

    assert(actualType == expectedTypeAfterAgg)
  }
}
