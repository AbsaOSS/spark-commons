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

package za.co.absa.spark.commons.errorhandler

import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandler.types.{AdditionalInfo, ErrCode, ErrMsg, ErrSourceColName, ErrType, ErrorColumn, ErrorWhen}
import scala.language.implicitConversions

/**
 * Class implement the functionality of implicit ErrorHandler trait to DataFrame. This implementation take ErrorHandler trait as an
 * implicit which will allow easier usage for ErrorHandler trait.
 */
object DataFrameErrorHandlerImplicit {
  /**
   * This method implicitly convert an errorColumn to a normal Column
   *
   * @param errorColumn error column that has to be converted to column
   * @return the errorColumn details as a normal column
   */
  implicit def convertErrorColumnToColumn(errorColumn: ErrorColumn): Column = {
    errorColumn.column
  }

  implicit class DataFrameEnhancedWithErrorHandler(val dataFrame: DataFrame) extends AnyVal {

    /**
     * Applies the earlier collected [[types.ErrorColumn ErrorColumns]] to the provided [[org.apache.spark.sql.DataFrame spark.DataFrame]].
     *
     * @param errCols   - a list of [[types.ErrorColumn]] returned by previous calls of [[ErrorHandler!.createErrorAsColumn(errorMessageSubmit:za\.co\.absa\.spark\.commons\.errorhandler\.ErrorMessageSubmit)* createErrorAsColumn]]
     * @return - the original data frame with the error detection applied
     * @group Error Handler
     * @since 0.6.0
     */
    def applyErrorColumnsToDataFrame(errCols: ErrorColumn*)(implicit errorHandler: ErrorHandler): DataFrame = {
      errorHandler.applyErrorColumnsToDataFrame(dataFrame)(errCols: _*)
    }

    /**
     * The idea of this function is: "Put the error specified to the provided dataframe if the condition is true on the row."
     * The error is transformed to a column using the [[ErrorHandler.transformErrorSubmitToColumn]] method and applied to the data frame
     * if the "when" condition is true using the [[ErrorHandler.doApplyErrorColumnsToDataFrame]] method.
     *
     * @param when               - the condition that defines the error occurred on the row
     * @param errorMessageSubmit - the detected error specification
     * @return - the original [[org.apache.spark.sql.DataFrame spark.DataFrame]] with the error detection applied
     * @group Error Handler
     * @since 0.6.0
     */
    def putError(when: Column)(errorMessageSubmit: ErrorMessageSubmit)(implicit errorHandler: ErrorHandler): DataFrame = {
      errorHandler.putError(dataFrame)(when)(errorMessageSubmit)
    }

    /**
     * Same as [[putError]], but allows a series of pairs condition-error to be specified at once.
     * It should be noted, that once an error has been identified for a field on the row, no more conditions bound to that
     * field are evaluated.
     *
     * @param errorsWhen - the list of condition-error pairs, the condition are grouped by the field of the error submissions
     * @return - the original data frame with the error detection applied
     * @group Error Handler
     * @since 0.6.0
     */
    def putErrorsWithGrouping(errorsWhen: Seq[ErrorWhen])(implicit errorHandler: ErrorHandler): DataFrame = {
      errorHandler.putErrorsWithGrouping(dataFrame)(errorsWhen)
    }

    /**
     * Transforms an error information into a column expression. For cases when simple column expression condition used in
     * [[putError]] is not suitable for whatever reason.
     * The returned [[types.ErrorColumn]] should then be used in [[applyErrorColumnsToDataFrame]].
     *
     * @param errorMessageSubmit - the error specification
     * @return - [[types.ErrorColumn]] expression containing the error specification
     * @group Error Handler
     * @since 0.6.0
     */
    def createErrorAsColumn(errorMessageSubmit: ErrorMessageSubmit)(implicit errorHandler: ErrorHandler): ErrorColumn = {
      errorHandler.createErrorAsColumn(errorMessageSubmit)
    }

    /**
     * Same as the other [[ErrorHandler!.createErrorAsColumn(errorMessageSubmit:za\.co\.absa\.spark\.commons\.errorhandler\.ErrorMessageSubmit)* createErrorAsColumn(errorMessageSubmit: ErrorMessageSubmit)]], only providing the error specification
     * in decomposed state, not in the [[ErrorMessageSubmit]] trait form.
     *
     * @param errType          - word description of the type of the error
     * @param errCode          - number designation of the type of the error
     * @param errMessage       - human friendly description of the error
     * @param errSourceColName - the name of the column the error happened at
     * @param additionalInfo   - any optional additional info in JSON format
     * @return - [[types.ErrorColumn]] expression containing the error specification
     * @group Error Handler
     * @since 0.6.0
     */
    def createErrorAsColumn(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errSourceColName: Option[ErrSourceColName], additionalInfo: AdditionalInfo = None)
                           (implicit errorHandler: ErrorHandler): ErrorColumn = {
      errorHandler.createErrorAsColumn(errType, errCode, errMessage, errSourceColName, additionalInfo)
    }
  }

}
