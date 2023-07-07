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

import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.{ErrorHandling, ErrorMessageSubmit}
import za.co.absa.spark.commons.sql.functions.null_col

object ErrorHandlingIgnoringErrors extends ErrorHandling {

  /**
   * First of the few methods that needs to be coded in the trait implementation
   * The purpose of this method is to convert the error specification into a [[org.apache.spark.sql.Column spark.Column]] expression
   *
   * @param errorMessageSubmit - the error specification
   * @return - the error specification transformed into a column expression
   * @group Error Handling
   * @since 0.6.0
   */
  override protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column = {
    null_col
  }

  /**
   * Applies the provided columns to the incoming [[org.apache.spark.sql.DataFrame spark.DataFrame]]. Usually they might be aggregated in some way and attached
   * to the [[org.apache.spark.sql.DataFrame spark.DataFrame]], but any other operations are imaginable. Unless really bent, the incoming columns are those
   * produced by [[transformErrorSubmitToColumn]].
   * The idea here is that the error column contains information of the error that occurred on the row or is empty (NULL)
   * otherwise.
   * In each implementation calling the function to each column separately or in any grouping of columns should produce
   * the same result (with the exception of order of errors in the aggregation).
   *
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to apply the error columns to
   * @param errCols   - the list of error columns to be applied to the dataFrame
   * @return - the original data frame passed for aggregation
   */
  override protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    dataFrame
  }

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandling]] is structured.
   * This function provides the information on the structure of single error column
   *
   * @return -  the DataType of the column returned from `createErrorAsColumn` function
   */
  override def errorColumnType: DataType = NullType

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandling]] is structured.
   * This function describes what is the type of the column attached (if it didn't exists before) to the [[org.apache.spark.sql.DataFrame DataFrame]]
   *
   * @return - the DataType of the column containing the error info that is attached to the [[org.apache.spark.sql.DataFrame DataFrame]].
   */
  override def dataFrameColumnType: Option[DataType] = None
}
