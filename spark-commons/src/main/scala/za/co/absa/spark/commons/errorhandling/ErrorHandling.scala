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

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.spark.commons.errorhandling.implementations.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.errorhandling.types._

trait ErrorHandling {

  def register(sparkToRegisterTo: SparkSession): Unit = {}

  def putErrorToColumn(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errCol: Option[ErrSourceColName], additionalInfo: AdditionalInfo = None): ErrorColumn = {
    val toSubmit = errCol
      .map(errSourceColName => ErrorMessageSubmitOnColumn(errType, errCode, errMessage, errSourceColName, additionalInfo))
      .getOrElse(ErrorMessageSubmitWithoutColumn(errType, errCode, errMessage, additionalInfo))
    putErrorToColumn(toSubmit)
  }
  def putErrorToColumn(errorMessageSubmit: ErrorMessageSubmit): ErrorColumn

  def aggregateErrorColumns(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame

  def putError(dataFrame: DataFrame)(when: Column)(errorMessageSubmit: ErrorMessageSubmit): DataFrame = {
    putErrorsWithGrouping(dataFrame)(Seq(ErrorWhen(when, errorMessageSubmit)))
  }

  def putErrorsWithGrouping(dataFrame: DataFrame)(errorsWhen: Seq[ErrorWhen]): DataFrame

  def errorColumnType: DataType

  def errorColumnAggregationType: DataType
}

