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

package za.co.absa.spark.commons.errorhandler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, column, length}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandler.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.errorhandler.implementations.ErrorHandlerFilteringErrorRows
import za.co.absa.spark.commons.errorhandler.types.{AdditionalInfo, ErrCode, ErrMsg, ErrSourceColName, ErrType, ErrorColumn, ErrorWhen}

class DataFrameErrorHandlerImplicitTest extends AnyFunSuite with SparkTestBase {
  import DataFrameErrorHandlerImplicit._
  import spark.implicits._

  implicit private val errorHander: ErrorHandler = ErrorHandlerFilteringErrorRows

  private val col1Name = "id"
  private val col2Name = "name"
  implicit private val df: DataFrame = Seq((1, "John"), (2, "Jane"), (3, "Alice")).toDF(col1Name, col2Name)

  private type ResultDfRecordType = (Option[Integer], String)

  private def resultDfToResult(resultDf: DataFrame): List[ResultDfRecordType] = {
    resultDf.as[ResultDfRecordType].collect().sortBy(_._1).toList
  }

  test("convertErrorColumnToColumn should convert ErrorColumn to Column") {
    // Create an ErrorColumn
    val errorColumn = df.createErrorAsColumn("Test error 1", 1, "This is a test error", Some("newColumn"))
    val result = df.withColumn("newColumn", errorColumn)
    val expectedType = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("newColumn", BooleanType, nullable = false))
    )

    assert(result.columns.contains("newColumn"))
    assert(result.schema == expectedType)
  }

  test("applyErrorColumnsToDataFrame should return DataFrame with records that don't have errors on them") {
    val expectedResults: List[ResultDfRecordType] = List(
      (Some(1),"John"), (Some(2),"Jane")
    )

    val er1 = ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("Id cannot be null", 0, "This is a test error"))
    val er2 = ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the column is too big", col1Name))
    val er3 = ErrorWhen(length(col(col2Name)) > 4, ErrorMessageSubmitOnColumn("String too long", 5, "The text in the field is too long", col2Name))

    // The putErrorsWithGrouping calls the doAggregationErrorColumns method implemented in ErrorHandlerFilteringErrorRows object
    val resultsDf = df.putErrorsWithGrouping(List(er1, er2, er3))
    val results = resultDfToResult(resultsDf)

    assert(results == expectedResults)
  }

  test("putError should add error based on condition") {
    val expectedColumns = Seq("id", "name")
    val expectedSchema = StructType(Seq(
      StructField("id",IntegerType,nullable = false),
      StructField("name",StringType,nullable = true))
    )

    val resultDf = df.putError(col("id") > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", "id"))
    val resultsSchema = resultDf.schema

    assert(resultDf.columns.toSeq == expectedColumns)
    assert(resultsSchema == expectedSchema)
  }

  test("putError and putErrors does not group by together") {
    val expected: List[ResultDfRecordType] = List()

    df.putError(col(col1Name) > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", col1Name))

    val resultDf = df.putErrorsWithGrouping(Seq(
      ErrorWhen(col(col1Name).isNull, ErrorMessageSubmitWithoutColumn("WrongLine", 0, "This line is wrong")),
      ErrorWhen(col(col1Name) > 2, ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)),
      ErrorWhen(length(col(col2Name)) > 2, ErrorMessageSubmitOnColumn("String too long", 10, "The text in the field is too long", col2Name))
    ))
    val result = resultDfToResult(resultDf)

    assert(result == expected)
  }

  test("putErrorsWithGrouping should add errors to DataFrame based on conditions") {
    val expectedResults: List[ResultDfRecordType] = List(
      (Some(1), "John")
    )

    // Define multiple error conditions and their corresponding error message submits
    val errorConditions = Seq(
      ErrorWhen(df(col2Name).isNull, ErrorMessageSubmitWithoutColumn("Test error 3", 0, "This is a test error")),
      ErrorWhen(df(col2Name) === "Jane", ErrorMessageSubmitOnColumn("Invalid name", 1, "The value of the column is too big", col2Name)),
      ErrorWhen(length(col(col2Name)) > 4, ErrorMessageSubmitOnColumn("String too long", 5, "The text in the field is too long", col2Name))
    )

    // Add errors to the DataFrame based on the conditions
    val resultDf = df.putErrorsWithGrouping(errorConditions)
    val actualResults = resultDfToResult(resultDf)

    assert(actualResults == expectedResults)
    assert(actualResults.head == expectedResults.head)
  }

  test("createErrorAsColumn should return an ErrorColumn with the specified error message") {
    val errorMessageSubmit: ErrorMessageSubmit = ErrorMessageSubmitOnColumn("ValueTooBig", 1, "The value of the field is too big", col1Name)
    val expectedResults = ErrorColumn(column("true")).toString

    val result = df.createErrorAsColumn(errorMessageSubmit)

    assert(result.toString == expectedResults)
  }

  test("createErrorAsColumn should return an ErrorColumn with the specified error details") {
    val errType: ErrType = "Test error"
    val errCode: ErrCode = 2
    val errMessage: ErrMsg = "Error message"
    val errSourceColName: Option[ErrSourceColName] = Option("name")
    val additionalInfo: AdditionalInfo = None
    val expectedResults = "ErrorColumn(true)"

    val result = df.createErrorAsColumn(errType, errCode, errMessage, errSourceColName, additionalInfo)

    assert(result.toString == expectedResults)
  }

}
