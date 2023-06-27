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

package za.co.absa.spark.commons.errorhandling

import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.errorhandling.implementations.{ErrorHandlingFilterRowsWithErrors, ErrorMessageArray}
import za.co.absa.spark.commons.errorhandling.partials.TransformIntoErrorMessage.FieldNames.{additionInfo, errCode, errColsAndValues, errMsg, errType}
import za.co.absa.spark.commons.errorhandling.types.ErrorWhen

class DataFrameErrorHandlingImplicitTest extends AnyFunSuite with SparkTestBase {
  import DataFrameErrorHandlingImplicit._
  import spark.implicits._

//  implicit private val errorHandling: ErrorHandling = ErrorHandlingFilterRowsWithErrors
  implicit private val errorHandling: ErrorHandling = new ErrorMessageArray("errColumn")

  private val col1Name = "id"
  private val col2Name = "name"
  private val errColName = "errColumn"
  private val df = Seq((1, "John"), (2, "Jane"), (3, "Alice")).toDF(col1Name, col2Name)

  test("applyErrorColumnsToDataFrame should apply error columns to DataFrame") {
//    implicit val errorHandling: ErrorHandling = new ErrorMessageArray("errColumn")

    val errorColumn = df.createErrorAsColumn("Test error 1", 1, "This is a test error", None)
    val result = df.applyErrorColumnsToDataFrame(errorColumn)

    assert(result.columns.contains(errColName))
    assert(result.count() == df.count())
  }

  test("putError should add error based on condition") {
    val expectedColumns = Seq("id", "name", "errColumn")
    val expectedSchema = StructType(Seq(
      StructField("id",IntegerType,false),
      StructField("name",StringType,true),
      StructField("errColumn",ArrayType(
        StructType(Seq(
          StructField(errType,StringType,true),
          StructField(errCode,LongType,true),
          StructField(errMsg,StringType,true),
          StructField(errColsAndValues,MapType(StringType,StringType,true),true),
          StructField(additionInfo,StringType,true))),false
      ),false))
    )

    val resultDf = df.putError(col("id") > 1)(ErrorMessageSubmitOnColumn("ValueStillTooBig", 2, "The value of the field is too big", "id"))

    assert(resultDf.columns.toSeq == expectedColumns)
    assert(resultDf.schema == expectedSchema)
  }


  test("putErrorsWithGrouping should add errors to DataFrame based on conditions") {
    // Define multiple error conditions and their corresponding error message submits
    val expectedErrOnJane = "List([Invalid name,1,The value of the column is too big,Map(name -> Jane),null])"
    val expectedErrOnJohn = "List()"
    val expectedErrOnAlice = "List([Invalid name,1,The value of the column is too big,Map(name -> Jane),null])"
    val expectedDfSchema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("errColumn", ArrayType(
        StructType(Seq(
          StructField(errType, StringType, true),
          StructField(errCode, LongType, true),
          StructField(errMsg, StringType, true),
          StructField(errColsAndValues, MapType(StringType, StringType, true), true),
          StructField(additionInfo, StringType, true))), false
      ), false))
    )

    val errorConditions = Seq(
      ErrorWhen(df(col2Name).isNull, ErrorMessageSubmitWithoutColumn("Test error 3", 0, "This is a test error")),
      ErrorWhen(df(col2Name) === "Jane", ErrorMessageSubmitOnColumn("Invalid name", 1, "The value of the column is too big", col2Name)),
      ErrorWhen(length(col(col2Name)) > 4, ErrorMessageSubmitOnColumn("String too long", 5, "The text in the field is too long", col2Name))
    )

    // Add errors to the DataFrame based on the conditions
    val resultDf = df.putErrorsWithGrouping(errorConditions)

    val actualErrOnJane = resultDf.select("errColumn").filter("name = 'Jane'").first().getSeq(0).toList
    val actualErrOnJohn = resultDf.select("errColumn").filter("name = 'John'").first().getSeq(0).toList
    val actualErrOnAlice = resultDf.select("errColumn").filter("name = 'Jane'").first().getSeq(0).toList

    // Verify that the errors are correctly added to the DataFrame
    assert(actualErrOnJane.toString() == expectedErrOnJane)
    assert(actualErrOnJohn.toString == expectedErrOnJohn)
    assert(actualErrOnAlice.toString() == expectedErrOnAlice)
    assert(resultDf.schema == expectedDfSchema)
  }

  test("convertErrorColumnToColumn should convert ErrorColumn to Column") {
    implicit val errorHandling: ErrorHandling = ErrorHandlingFilterRowsWithErrors

    // Create an ErrorColumn
    val errorColumn = df.createErrorAsColumn("Test error 1", 1, "This is a test error", Some("newColumn"))
    val result = df.withColumn("newColumn", errorColumn)

    assert(result.columns.contains("newColumn"))
  }

}
