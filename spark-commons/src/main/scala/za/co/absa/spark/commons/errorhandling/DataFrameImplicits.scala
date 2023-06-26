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

import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.types.{AdditionalInfo, ErrCode, ErrMsg, ErrSourceColName, ErrType, ErrorColumn, ErrorWhen}

object DataFrameImplicits {
  implicit class ErrorHandlingDataFrameImplicit(dataFrame: DataFrame)(implicit errorHandling: ErrorHandling){
    def applyErrorColumnsToDataFrame(errCols: ErrorColumn*): DataFrame = {
      errorHandling.applyErrorColumnsToDataFrame(dataFrame)(errCols: _*)
    }

    def putError(when: Column)(errorMessageSubmit: ErrorMessageSubmit): DataFrame = {
      errorHandling.putError(dataFrame)(when)(errorMessageSubmit)
    }

    def putErrorsWithGrouping(errorsWhen: Seq[ErrorWhen]): DataFrame = {
      errorHandling.putErrorsWithGrouping(dataFrame)(errorsWhen)
    }

    def createErrorAsColumn(errorMessageSubmit: ErrorMessageSubmit): ErrorColumn = {
      errorHandling.createErrorAsColumn(errorMessageSubmit)
    }

    def createErrorAsColumn(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errSourceColName: Option[ErrSourceColName], additionalInfo: AdditionalInfo = None): ErrorColumn = {
      errorHandling.createErrorAsColumn(errType, errCode, errMessage, errSourceColName, additionalInfo)
    }

    implicit def convertErrorColumnToColumn(errorColumn: ErrorColumn): Column = {
      errorColumn.column
    }
  }
}
