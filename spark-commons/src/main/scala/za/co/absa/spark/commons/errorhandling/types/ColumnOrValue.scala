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

/**
 * Class to unify a representation of a [[za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit ErrorMessageSubmit]] segments.
 * It can be build from `column`,column name or a set of column names, a constant value and others.
 * The class then provides the ability to express each option as a Spark column used in other [[za.co.absa.spark.commons.errorhandling.ErrorHandling ErrorHandling]]
 * classes and methods.
 * @tparam T - The type of the value and the Scala equivalent of the column DataType
 * @group Error Handling
 * @since 0.6.0
 */
trait ColumnOrValue[T] {
  /**
   * @return `column` expression representing the input
   * @group Error Handling
   * @since 0.6.0
   */
  def column: Column

  /**
   * @return the name or names if columns are directly referenced.
   * @group Error Handling
   * @since 0.6.0
   */
  def columnNames: Set[String]

  /**
   * @return the constant value if entity was build from one, otherwise `None`
   * @group Error Handling
   * @since 0.6.0
   */

  def getValue: Option[T]
}

object ColumnOrValue {
  /**
   * Just a shorthand alias of [[ColumnOrValue]], for less typying
   * @tparam T - The type of the value and the Scala equivalent of the column DataType
   * @group Error Handling
   * @since 0.6.0
   */
  type CoV[T] = ColumnOrValue[T] //just a shorthand
  val CoV: ColumnOrValue.type = ColumnOrValue

  /**
   * Referencing exactly one column, by its name
   * @param columnName - the column name
   * @tparam T - The Scala type equivalent to the column `DataType`
   * @group Error Handling
   * @since 0.6.0
   */
  def apply[T](columnName: String): ColumnOrValue[T] = CoVNamedColumn(columnName)

  /**
   * Referencing a column by its expression
   * @param column - the column expression
   * @tparam T - The Scala type equivalent to the column `DataType`
   * @group Error Handling
   * @since 0.6.0
   */
  def apply[T](column: Column): ColumnOrValue[T] = CoVDefinedColumn(column)

  /**
   * Referencing a column which is a map of column names and their values transformed by the transformer
   * @param mapColumnNames - the column names in the map
   * @param columnTransformer - function to tranform the column values with
   * @tparam T - The Scala type equivalent to the column `DataType`
   * @group Error Handling
   * @since 0.6.0
   */
  def apply[T](mapColumnNames: Set[String], columnTransformer: ColumnTransformer): ColumnOrValue[Map[String, T]] = {
    CoVMapColumn(mapColumnNames, columnTransformer)
  }

  /**
   * Representing and optional string value - String or NULL
   * @param value - the value to represent in the constant column or NULL if None
   * @group Error Handling
   * @since 0.6.0
   */
  def withOption(value: Option[String]): ColumnOrValue[Option[String]] = { // could be safely an apply, or done more generally
    value match {
      case None => CoVNull(StringType)
      case Some(x) => CoVOption(x)
    }
  }

  /**
   * Referencing a constant value
   * @param value - the constant the column to represent
   * @tparam T - The Scala type equivalent to the column `DataType`
   * @group Error Handling
   * @since 0.6.0
   */
  def withValue[T](value: T): ColumnOrValue[T] = CoVValue(value)

  /**
   * @return - column of NULL values as StringType
   * @group Error Handling
   * @since 0.6.0
   */
  def asEmpty: ColumnOrValue[Option[String]] = CoVNull(StringType)

  /**
   * Referencing a column which is a map of column names and their values casted to string
   * @param mapColumnNames - the column names in the map
   * @group Error Handling
   * @since 0.6.0
   */
  def asMapOfStringColumns(mapColumnNames: Set[String]): ColumnOrValue[Map[String, String]] = CoVMapColumn(mapColumnNames, columnNameToItsStringValue)

  private def columnNameToItsStringValue(colName: String): Column = col(colName).cast(StringType)

  private final case class CoVNamedColumn[T](columnName: String) extends ColumnOrValue[T] {
    val column: Column = col(columnName)
    val columnNames: Set[String] = Set(columnName)
    val getValue: Option[T] = None
  }

  private final case class CoVDefinedColumn[T](column: Column) extends ColumnOrValue[T] {
    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = None
  }

  private final case class CoVValue[T](value: T) extends ColumnOrValue[T] {
    val column: Column = lit(value)
    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = Option(value)
  }

  private final case class CoVMapColumn[T](columnNames: Set[String], columnTransformer: ColumnTransformer) extends ColumnOrValue[Map[String, T]] {
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

    val columnNames: Set[String] = Set.empty
    val getValue: Option[Option[T]] = Some(Some(value))
  }

  private final case class CoVNull[T](dataType: DataType) extends ColumnOrValue[T] {
    val column: Column = null_col(dataType)

    val columnNames: Set[String] = Set.empty
    val getValue: Option[T] = None
  }
}
