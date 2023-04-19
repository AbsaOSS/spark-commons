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
import org.apache.spark.sql.functions.{col, lit}
import za.co.absa.spark.commons.sql.functions.null_col

import scala.language.higherKinds

trait ColumnOrValue[T] {
  def column: Column
  def isColumn: Boolean
  def isValue: Boolean
  def getColumnName: Option[String]
  def getValue: Option[T]
}

object ColumnOrValue {
  type CoV[T] = ColumnOrValue[T] //just a shorthand
  val CoV: ColumnOrValue.type = ColumnOrValue

  def apply[T](columnName: String): ColumnOrValue[T] = CoVNamedColumn(columnName)
  def apply[T](column: Column): ColumnOrValue[T] = CoVDefinedColumn(column)
  def apply[T](values: Seq[T]): ColumnOrValue[Seq[T]] = ??? //TODO
  def apply[T](value: Option[T]): ColumnOrValue[Option[T]] = ??? //TODO

  def withOption[T](value: Option[T]): ColumnOrValue[Option[T]] = {
    value match {
      case None => CoVNull()
      case Some(x) => CoVOption(x)
    }
  }
  def withActualValue[T](value: T): ColumnOrValue[T] = CoVValue(value)
  def asEmpty[T]: ColumnOrValue[T] = CoVNull()

  private final case class CoVNamedColumn[T](columnName: String) extends ColumnOrValue[T] {
    val column: Column = col(columnName)
    val isColumn: Boolean = true
    val isValue: Boolean = false
    val getColumnName: Option[String] = Option(columnName)
    val getValue: Option[T] = None
  }

  private final case class CoVDefinedColumn[T](column: Column) extends ColumnOrValue[T] {
    val isColumn: Boolean = true
    val isValue: Boolean = false
    val getColumnName: Option[ErrType] = None
    val getValue: Option[T] = None
  }

  private final case class CoVValue[T](value: T) extends ColumnOrValue[T] {
    val column: Column = lit(value)
    val isColumn: Boolean = false
    val isValue: Boolean = true
    val getColumnName: Option[String] = None
    val getValue: Option[T] = Option(value)
  }

  private final case class CoVOption[T](value: T) extends ColumnOrValue[Option[T]] {
    val column: Column = lit(value)
    val isColumn: Boolean = false
    val isValue: Boolean = true
    val getColumnName: Option[String] = None
    val getValue: Option[Option[T]] = Some(Some(value))
  }

  private final case class CoVNull[T]() extends ColumnOrValue[T] {
    val column: Column = null_col
    val isColumn: Boolean = false
    val isValue: Boolean = true
    val getColumnName: Option[String] = None
    val getValue: Option[T] = None
  }
}
