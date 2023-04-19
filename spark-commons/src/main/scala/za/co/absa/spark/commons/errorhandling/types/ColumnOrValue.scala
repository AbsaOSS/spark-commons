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
import org.apache.spark.sql.functions.{array, col, lit, map_from_arrays, typedLit}
import org.apache.spark.sql.types.{DataType, StringType}
import za.co.absa.spark.commons.sql.functions.null_col

import scala.language.higherKinds

trait ColumnOrValue[T] {
  def column: Column
  def isColumn: Boolean
  def isValue: Boolean
  def columnNames: Set[String]
  def getValue: Option[T]
}

object ColumnOrValue {
  type CoV[T] = ColumnOrValue[T] //just a shorthand
  val CoV: ColumnOrValue.type = ColumnOrValue

  def apply[T](columnName: String): ColumnOrValue[T] = CoVNamedColumn(columnName)
  def apply[T](column: Column): ColumnOrValue[T] = CoVDefinedColumn(column)
  def apply[T](mapColumnNames: Set[String], columnTransformer: ColumnTransformer): ColumnOrValue[Map[String, T]] = CoVMapColumn(mapColumnNames, columnTransformer) //should it be explicit function?

  def withOption(value: Option[String]): ColumnOrValue[Option[String]] = { // could be safely an apply, or done more generally
    value match {
      case None => CoVNull(StringType)
      case Some(x) => CoVOption(x)
    }
  }
  def withValue[T](value: T): ColumnOrValue[T] = CoVValue(value)

  def asEmpty: ColumnOrValue[Option[String]] = CoVNull(StringType)
  def asMapOfStringColumns(mapColumnNames: Set[String]): ColumnOrValue[Map[String, String]] = CoVMapColumn(mapColumnNames, columnNameToItsStringValue)

  def columnNameToItsStringValue(colName: String): Column = col(colName).cast(StringType)

  private final case class CoVNamedColumn[T](columnName: String) extends ColumnOrValue[T] {
    val column: Column = col(columnName)
    val isColumn: Boolean = true
    val isValue: Boolean = false
    val columnNames: Set[String] = Set(columnName)
    val getValue: Option[T] = None
  }

  private final case class CoVDefinedColumn[T](column: Column) extends ColumnOrValue[T] {
    val isColumn: Boolean = true
    val isValue: Boolean = false
    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = None
  }

  private final case class CoVValue[T](value: T) extends ColumnOrValue[T] {
    val column: Column = lit(value)
    val isColumn: Boolean = false
    val isValue: Boolean = true
    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = Option(value)
  }

  private final case class CoVMapColumn[T](columnNames: Set[String], columnTransformer: ColumnTransformer) extends ColumnOrValue[Map[String, T]] {
    val isColumn: Boolean = true
    val isValue: Boolean = false
    val getValue: Option[Map[String, T]] = None
    val column: Column = {
      val (mapKeys, mapValues) = columnNames.foldRight(Seq.empty[Column], Seq.empty[Column]) {case (colName, (accKeys, accVals)) =>
        (typedLit(colName) +: accKeys , columnTransformer(colName) +: accVals)
      }
      map_from_arrays(array(mapKeys: _*), array(mapValues: _*))
    }
  }

  private final case class CoVOption[T](value: T) extends ColumnOrValue[Option[T]] {
    val column: Column = lit(value)

    val isColumn: Boolean = false
    val isValue: Boolean = true
    val columnNames: Set[String] = Set.empty
    val getValue: Option[Option[T]] = Some(Some(value))
  }

  private final case class CoVNull[T](dataType: DataType) extends ColumnOrValue[T] {
    val column: Column = null_col(dataType)

    val isColumn: Boolean = false
    val isValue: Boolean = true
    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = None
  }
}
