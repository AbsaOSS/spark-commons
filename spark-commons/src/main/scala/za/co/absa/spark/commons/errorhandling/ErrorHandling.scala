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

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.implementations.submits.{ErrorMessageSubmitOnColumn, ErrorMessageSubmitWithoutColumn}
import za.co.absa.spark.commons.errorhandling.types._

/**
 * The basic class of error handling component. Every library that wants to use the component during Spark data
 * processing should utilize this trait and its methods. The methods serve to record the errors and attach them to the
 * [[org.apache.spark.sql.DataFrame spark.DataFrame]]. The trait should be an input parameter for such library, perhaps as an implicit.
 * On the other side the end application provides concrete [[ErrorHandling]] implementation, that does the actual error
 * handling by the application desire.
 * For easy to use and as examples, a few general implementations are provided in the implementations sub-folder.
 * Also for common, repeated implementations the folder `partials` offer some traits.
 */
trait ErrorHandling {
  /**
   * First of the few methods that needs to be coded in the trait implementation
   * The purpose of this method is to convert the error specification into a [[org.apache.spark.sql.Column spark.Column]] expression
   * @param errorMessageSubmit - the error specification
   * @return - the error specification transformed into a column expression
   * @group Error Handling
   * @since 0.6.0
   */
  protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column

  /**
   * Applies the provided columns to the incoming [[org.apache.spark.sql.DataFrame spark.DataFrame]]. Usually they might be aggregated in some way and attached
   * to the [[org.apache.spark.sql.DataFrame spark.DataFrame]], but any other operations are imaginable. Unless really bent, the incoming columns are those
   * produced by [[transformErrorSubmitToColumn]].
   * The idea here is that the error column contains information of the error that occurred on the row or is empty (NULL)
   * otherwise.
   * In each implementation calling the function to each column separately or in any grouping of columns should produce
   * the same result (with the exception of order of errors in the aggregation).
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to apply the error columns to
   * @param errCols - the list of error columns to apply
   * @return - data frame with the error columns applied (aggregated and attached or done otherwise)
   */
  protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame

  /**
   * The idea of this function is: "Put the error specified to the provided dataframe if the condition is true on the row."
   * The error is transformed to a column using the [[transformErrorSubmitToColumn]] method and applied to the data frame
   * if the "when" condition is true using the [[doApplyErrorColumnsToDataFrame]] method.
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to operate on
   * @param when - the condition that defines the error occurred on the row
   * @param errorMessageSubmit - the detected error specification
   * @return - the original [[org.apache.spark.sql.DataFrame spark.DataFrame]] with the error detection applied
   * @group Error Handling
   * @since 0.6.0
   */
  def putError(dataFrame: DataFrame)(when: Column)(errorMessageSubmit: ErrorMessageSubmit): DataFrame = {
    putErrorsWithGrouping(dataFrame)(Seq(ErrorWhen(when, errorMessageSubmit)))
  }

  /**
   * Same as [[putError]], but allows a series of pairs condition-error to be specified at once.
   * It should be noted, that once an error has been identified for a field on the row, no more conditions bound to that
   * field are evaluated.
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to operate on
   * @param errorsWhen - the list of condition-error pairs, the condition are grouped by the field of the error submissions
   * @return - the original data frame with the error detection applied
   * @group Error Handling
   * @since 0.6.0
   */
  def putErrorsWithGrouping(dataFrame: DataFrame)(errorsWhen: Seq[ErrorWhen]): DataFrame = {
    def errorWhenToCol(errorWhen: ErrorWhen): Column = {
      when(errorWhen.when, transformErrorSubmitToColumn(errorWhen.errorMessageSubmit))
    }
    def errorWhenSeqToCol(errorsWhen: Seq[ErrorWhen]): Column = {
      val branches: Seq[(Expression, Expression)] = errorsWhen.map(errorWhen => (errorWhen.when.expr, transformErrorSubmitToColumn(errorWhen.errorMessageSubmit).expr))
      new Column(CaseWhen(branches))
    }

    val errorsByColumn = errorsWhen.groupBy(_.errorMessageSubmit.errColsAndValues.columnNames)
    val noColNames = Set.empty[String]
    val errorColumns1 = errorsByColumn.getOrElse(noColNames, Seq.empty).map(errorWhenToCol) // no grouping without ErrCol names
    val errorColumns2 = (errorsByColumn - noColNames).values.map(errorWhenSeqToCol).toSeq
    doApplyErrorColumnsToDataFrame(dataFrame, errorColumns1 ++ errorColumns2: _*)
  }

  /**
   * Transforms an error information into a column expression. For cases when simple column expression condition used in
   * [[putError]] is not suitable for whatever reason.
   * The returned [[types.ErrorColumn]] should then be used in [[applyErrorColumnsToDataFrame]].
   * @param errorMessageSubmit - the error specification
   * @return - [[types.ErrorColumn]] expression containing the error specification
   * @group Error Handling
   * @since 0.6.0
   */
  def createErrorAsColumn(errorMessageSubmit: ErrorMessageSubmit): ErrorColumn = {
    ErrorColumn(transformErrorSubmitToColumn(errorMessageSubmit))
  }

  /**
   * Same as the other [[ErrorHandling!.createErrorAsColumn(errorMessageSubmit:za\.co\.absa\.spark\.commons\.errorhandling\.ErrorMessageSubmit)* createErrorAsColumn(errorMessageSubmit: ErrorMessageSubmit)]], only providing the error specification
   * in decomposed state, not in the [[ErrorMessageSubmit]] trait form.
   * @param errType - word description of the type of the error
   * @param errCode - number designation of the type of the error
   * @param errMessage - human friendly description of the error
   * @param errSourceColName - the name of the column the error happened at
   * @param additionalInfo - any optional additional info in JSON format
   * @return - [[types.ErrorColumn]] expression containing the error specification
   * @group Error Handling
   * @since 0.6.0
   */
  def createErrorAsColumn(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errSourceColName: Option[ErrSourceColName], additionalInfo: AdditionalInfo = None): ErrorColumn = {
    val toSubmit = errSourceColName
      .map(errSourceColName => ErrorMessageSubmitOnColumn(errType, errCode, errMessage, errSourceColName, additionalInfo))
      .getOrElse(ErrorMessageSubmitWithoutColumn(errType, errCode, errMessage, additionalInfo))
    createErrorAsColumn(toSubmit)
  }

  /**
   * Applies the earlier collected [[types.ErrorColumn ErrorColumns]] to the provided [[org.apache.spark.sql.DataFrame spark.DataFrame]].
   * See [[doApplyErrorColumnsToDataFrame]] for detailed functional explanation.
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to operate on
   * @param errCols - a list of [[types.ErrorColumn]] returned by previous calls of [[ErrorHandling!.createErrorAsColumn(errorMessageSubmit:za\.co\.absa\.spark\.commons\.errorhandling\.ErrorMessageSubmit)* createErrorAsColumn]]
   * @return - the original data frame with the error detection applied
   * @group Error Handling
   * @since 0.6.0
   */
  def applyErrorColumnsToDataFrame(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame = {
    doApplyErrorColumnsToDataFrame(dataFrame, errCols.map(_.column): _*)
  }
}
