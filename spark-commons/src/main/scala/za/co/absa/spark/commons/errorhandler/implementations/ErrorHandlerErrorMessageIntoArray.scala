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

package za.co.absa.spark.commons.errorhandler.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, array_except, array_union, col, map_from_arrays, map_keys, map_values, struct, when}
import org.apache.spark.sql.types.{ArrayType, DataType}
import za.co.absa.spark.commons.adapters.TransformAdapter
import za.co.absa.spark.commons.errorhandler.ErrorHandler
import za.co.absa.spark.commons.errorhandler.partials.TransformIntoErrorMessage.FieldNames._
import za.co.absa.spark.commons.errorhandler.partials.TransformIntoErrorMessage
import za.co.absa.spark.commons.sql.functions.null_col
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

/**
 * An implementation of [[ErrorHandler]] that collects errors into columns of struct based on [[za.co.absa.spark.commons.errorhandler.ErrorMessage ErrorMessage]] case class.
 * Upon applying  the non-NULL columns are aggregated into an array column which is attached to the [[org.apache.spark.sql.DataFrame spark.DataFrame]].
 * In case the column already exists in the DataFrame, the columns are appended to the column.
 *
 * @param errorColumnName - the name of the array column aggregating all the errors
 */
case class ErrorHandlerErrorMessageIntoArray(errorColumnName: String = ErrorHandlerErrorMessageIntoArray.defaultErrorColumnName)
  extends ErrorHandler
  with TransformIntoErrorMessage
  with TransformAdapter {

  private def decomposeMap(errorMessageColumn: Column): Column = {
    when(errorMessageColumn.isNotNull,
      struct(
        errorMessageColumn.getField(errType) as errType,
        errorMessageColumn.getField(errCode) as errCode,
        errorMessageColumn.getField(errMsg) as errMsg,
        map_keys(errorMessageColumn.getField(errColsAndValues)) as errSourceCols,
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
      map_from_arrays(errorMessageColumn.getField(errSourceCols), errorMessageColumn.getField(errValues)) as errColsAndValues,
      errorMessageColumn.getField(additionInfo) as additionInfo
    )
  }

  private def deMap(arrayCol: Column): Column = transform(arrayCol, decomposeMap)
  private def reMap(arrayCol: Column): Column = transform(arrayCol, recomposeMap)

  private def appendToErrCol(dataFrame: DataFrame, errorColName: String, colToUnion: Column): DataFrame = {
    dataFrame.withColumn(errorColName, reMap(array_union(deMap(col(errorColName)), colToUnion)))
  }

  protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregated = array(errCols.map(decomposeMap): _*) //need to decompose the map field, as it's not supported in array functions
    val aggregatedWithoutNulls = array_except(aggregated, array(null_col))
    val joinToExisting: (DataFrame, String) => DataFrame = appendToErrCol(_, _, aggregatedWithoutNulls)
    dataFrame.withColumnIfDoesNotExist(joinToExisting)(errorColumnName, reMap(aggregatedWithoutNulls))
  }

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandler]] is structured.
   * This function describes what is the type of the column attached (if it didn't exists before) to the [[org.apache.spark.sql.DataFrame DataFrame]]
   *
   * @return - the aggregated [[za.co.absa.spark.commons.errorhandler.ErrorHandler.errorColumnType ErrorHandler.errorColumnType]] into an ArrayType
   */
  override def dataFrameColumnType: Option[DataType] = {
    Option(ArrayType(errorColumnType, containsNull = false))
  }

}

object ErrorHandlerErrorMessageIntoArray {
  final val defaultErrorColumnName = "errCol"
}
