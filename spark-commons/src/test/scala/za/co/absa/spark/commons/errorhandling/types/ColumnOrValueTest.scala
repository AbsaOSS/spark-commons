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

package za.co.absa.spark.commons.errorhandling.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, col, current_date, lit, map_from_arrays}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.sql.functions.null_col

class ColumnOrValueTest extends AnyFunSuite {
  test("Creation of column based on its name"){
    val colName = "my_column"
    val expected = ColumnOrValueForm(col(colName), Set(colName), None)
    val result = ColumnOrValue(colName)
    expected assertTo result
  }

  test("Creation of column based on its definition") {
    val myColumn = current_date
    val expected = ColumnOrValueForm(myColumn, Set(), None)
    val result = ColumnOrValue(myColumn)
    expected assertTo result
  }

  test("Creation of map from given column names") {
    val colNames = Set("Col1", "Col2", "Col3")
    val colTransformer: String => Column = col
    val expectedColumn = map_from_arrays(
      array(
        lit("Col1"), lit("Col2"), lit("Col3")
      ), array(
        col("Col1"), col("Col2"), col("Col3")
      ))
    val expected = ColumnOrValueForm[Map[String, Any]](expectedColumn, colNames, None)
    val result = ColumnOrValue[Any](colNames, colTransformer)
    expected assertTo result
  }

  test("Creating ColumnOrValue from a defined Option") {
    val value = "Foo"
    val expected = ColumnOrValueForm(lit(value), Set(), Option(Option(value)))
    val result = ColumnOrValue.withOption(Option(value))
    expected assertTo result
  }

  test("Creating ColumnOrValue from an empty Option") {
    val expected = ColumnOrValueForm[Option[String]](null_col(StringType), Set(), None)
    val result = ColumnOrValue.withOption(None)
    expected assertTo result
  }

  test("Creating ColumnOrValue from a given value") {
    val value = 42
    val expected = ColumnOrValueForm(lit(value), Set(), Some(value))
    val result = ColumnOrValue.withValue(value)
    expected assertTo result
  }

  test("Creating ColumnOrValue as an undefined (empty) value") {

    val myColumn = null_col(StringType)
    val expected = ColumnOrValueForm[Option[String]](myColumn, Set(), None)
    val result = ColumnOrValue.asEmpty
    expected assertTo result
  }

  test("Creating ColumnOrValue as a map of string columns") {
    val colNames = Set("Col1", "Col2", "Col3")
    val expectedColumn = map_from_arrays(
      array(
        lit("Col1"), lit("Col2"), lit("Col3")
      ), array(
        col("Col1").cast(StringType), col("Col2").cast(StringType), col("Col3").cast(StringType)
      ))
    val expected = ColumnOrValueForm[Map[String, String]](expectedColumn, colNames, None)
    val result = ColumnOrValue.asMapOfStringColumns(colNames)
    expected assertTo result
  }

}
