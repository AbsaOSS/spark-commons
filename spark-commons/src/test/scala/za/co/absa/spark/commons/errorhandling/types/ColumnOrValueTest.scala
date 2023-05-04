/*
 * Copyright 2023 ABSA Group Limited
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

<<<<<<< HEAD
import net.bytebuddy.dynamic.scaffold.MethodGraph.Empty
=======
>>>>>>> master
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, col, current_date, lit, map_from_arrays}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.sql.functions.null_col

class ColumnOrValueTest extends AnyFunSuite {
  test("Creation of column based on its name"){
    val colName = "my_column"
<<<<<<< HEAD
    val expected = ColumnOrValueForm(col(colName), isColumn = true, isValue = false, Set(colName), None)
=======
    val expected = ColumnOrValueForm(col(colName), Set(colName), None)
>>>>>>> master
    val result = ColumnOrValue(colName)
    expected assertTo result
  }

  test("Creation of column based on its definition") {
    val myColumn = current_date
<<<<<<< HEAD
    val expected = ColumnOrValueForm(myColumn, isColumn = true, isValue = false, Set(), None)
=======
    val expected = ColumnOrValueForm(myColumn, Set(), None)
>>>>>>> master
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
<<<<<<< HEAD
    val expected = ColumnOrValueForm[Map[String, Any]](expectedColumn, isColumn = true, isValue = false, colNames, None)
    val result = ColumnOrValue[Any](colNames, colTransformer)
    expected assertTo(result)
=======
    val expected = ColumnOrValueForm[Map[String, Any]](expectedColumn, colNames, None)
    val result = ColumnOrValue[Any](colNames, colTransformer)
    expected assertTo result
>>>>>>> master
  }

  test("Creating ColumnOrValue from a defined Option") {
    val value = "Foo"
<<<<<<< HEAD
    val expected = ColumnOrValueForm(lit(value), isColumn = false, isValue = true, Set(), Option(Option(value)))
=======
    val expected = ColumnOrValueForm(lit(value), Set(), Option(Option(value)))
>>>>>>> master
    val result = ColumnOrValue.withOption(Option(value))
    expected assertTo result
  }

  test("Creating ColumnOrValue from an empty Option") {
<<<<<<< HEAD
    val expected = ColumnOrValueForm[Option[String]](null_col(StringType), isColumn = false, isValue = true, Set(), None)
=======
    val expected = ColumnOrValueForm[Option[String]](null_col(StringType), Set(), None)
>>>>>>> master
    val result = ColumnOrValue.withOption(None)
    expected assertTo result
  }

  test("Creating ColumnOrValue from a given value") {
    val value = 42
<<<<<<< HEAD
    val expected = ColumnOrValueForm(lit(value), isColumn = false, isValue = true, Set(), Some(value))
=======
    val expected = ColumnOrValueForm(lit(value), Set(), Some(value))
>>>>>>> master
    val result = ColumnOrValue.withValue(value)
    expected assertTo result
  }

  test("Creating ColumnOrValue as an undefined (empty) value") {

    val myColumn = null_col(StringType)
<<<<<<< HEAD
    val expected = ColumnOrValueForm[Option[String]](myColumn, isColumn = false, isValue = true, Set(), None)
=======
    val expected = ColumnOrValueForm[Option[String]](myColumn, Set(), None)
>>>>>>> master
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
<<<<<<< HEAD
    val expected = ColumnOrValueForm[Map[String, String]](expectedColumn, isColumn = true, isValue = false, colNames, None)
    val result = ColumnOrValue.asMapOfStringColumns(colNames)
    expected assertTo(result)
=======
    val expected = ColumnOrValueForm[Map[String, String]](expectedColumn, colNames, None)
    val result = ColumnOrValue.asMapOfStringColumns(colNames)
    expected assertTo result
>>>>>>> master
  }


}
