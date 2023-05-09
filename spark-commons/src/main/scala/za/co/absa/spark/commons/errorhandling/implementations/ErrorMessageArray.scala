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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, array_except, array_union, col, column, map_from_arrays, map_keys, map_values, struct, when}
import org.apache.spark.sql.types.DataType
import za.co.absa.spark.commons.adapters.TransformAdapter
import za.co.absa.spark.commons.errorhandling.partials.EvaluateIntoErrorMessage.FieldNames._
import za.co.absa.spark.commons.errorhandling.partials.{ErrorHandlingCommon, EvaluateIntoErrorMessage}
import za.co.absa.spark.commons.errorhandling.types.ErrorColumn
import za.co.absa.spark.commons.sql.functions.null_col
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

case class ErrorMessageArray(errorColumnName: String = ErrorMessageArray.defaultErrorColumnName)
  extends ErrorHandlingCommon
  with EvaluateIntoErrorMessage
  with TransformAdapter {

  private def decomposeMap(errorMessageColumn: Column): Column = {
    when(errorMessageColumn.isNotNull,
      struct(
        errorMessageColumn.getField(errType) as errType,
        errorMessageColumn.getField(errCode) as errCode,
        errorMessageColumn.getField(errMsg) as errMsg,
        map_keys(errorMessageColumn.getField(errColsAndValues)) as errCols,
        map_values(errorMessageColumn.getField(errColsAndValues)) as errValues,
        errorMessageColumn.getField(additionInfo) as additionInfo
      )
    )
  }

  private def recomposeMap(errorMessageColumn: Column): Column = {
    struct(
      errorMessageColumn.getField(errType) as errType,
      errorMessageColumn.getField(errCode) as errCode,
      errorMessageColumn.getField(errMsg) as errMsg,
      map_from_arrays(errorMessageColumn.getField(errCols), errorMessageColumn.getField(errValues)) as errColsAndValues,
      errorMessageColumn.getField(additionInfo) as additionInfo
    )
  }

  private def deMap(arrayCol: Column): Column = transform(arrayCol, decomposeMap)
  private def reMap(arrayCol: Column): Column = transform(arrayCol, recomposeMap)

  private def appendToErrCol(dataFrame: DataFrame, errorColName: String, colToUnion: Column): DataFrame = {
    dataFrame.withColumn(errorColName, reMap(array_union(deMap(col(errorColName)), colToUnion)))
  }

  protected def doTheColumnsAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregated = array(errCols.map(decomposeMap): _*) //need to decompose the map field, as it's not supported in array functions
    val aggregatedWithoutNulls = array_except(aggregated, array(null_col))
    val joinToExisting: (DataFrame, String) => DataFrame = appendToErrCol(_, _, aggregatedWithoutNulls)
    dataFrame.withColumnIfDoesNotExist(joinToExisting)(errorColumnName, reMap(aggregatedWithoutNulls))
  }

  override def errorColumnType: DataType = {
    col(errorColumnName).expr.dataType
  }

  override def errorColumnAggregationType(aggregatedDF: DataFrame): Option[DataType] = {
    val hasErrorColumn = aggregatedDF.columns.contains(errorColumnName)

    if (hasErrorColumn == true) {
      val errorType = aggregatedDF.select(col(errorColumnName)).schema.fields.head.dataType
      Some(errorType)
    } else {
      None
    }
  }

}

object ErrorMessageArray {
  final val defaultErrorColumnName = "errCol"
}
