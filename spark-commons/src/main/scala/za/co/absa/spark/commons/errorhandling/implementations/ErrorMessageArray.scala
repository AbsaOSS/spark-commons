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

import org.apache.spark.sql.functions.{array, array_except, array_union, col}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.partials.{ErrorHandlingCommon, EvaluateIntoErrorMessage}
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.sql.functions.null_col

case class ErrorMessageArray(errorColumnName: String = ErrorMessageArray.defaultErrorColumnName)
  extends ErrorHandlingCommon
  with EvaluateIntoErrorMessage {

  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    def appendToArray(dataFrame: DataFrame, colName: String, colToUnion: Column): DataFrame = {
      dataFrame.withColumn(colName, array_union(col(colName), colToUnion))
    }
    val aggregatedWithoutNulls = array_except(array(errCols: _*), array(null_col))
    dataFrame.withColumnIfDoesNotExist(appendToArray(_, _, aggregatedWithoutNulls))(errorColumnName, aggregatedWithoutNulls)
  }

}

object ErrorMessageArray {
  final val defaultErrorColumnName = "errCol"
}
