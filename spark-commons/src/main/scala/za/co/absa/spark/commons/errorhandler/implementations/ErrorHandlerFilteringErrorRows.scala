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
import org.apache.spark.sql.functions.{coalesce, lit}
import org.apache.spark.sql.types.{BooleanType, DataType}
import za.co.absa.spark.commons.errorhandler.{ErrorHandler, ErrorMessageSubmit}

/**
 * Class implements the functionality of filtering rows that have some error (any of the error columns is not NULL).
 */
object ErrorHandlerFilteringErrorRows extends ErrorHandler {

  /**
   * Creates a column with the error description, in this particular case actually only signals with a boolean flag there was an error in the row.
   * @param errorMessageSubmit - the description of the error
   * @return - A column with boolean value indicating there was an error on the row.
   */
  override protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column = {
    lit(true)
  }

  /**
   * Filters out rows that contain at least one true flag in the provided boolean error columns
   * @param dataFrame - The data frame to filter on
   * @param errCols - the error columns to signal if the row should be filtered or not
   * @return - returns the dataframe without rows with errors
   */
  override protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val columns: Seq[Column] = errCols :+ lit(false)
    dataFrame.filter(!coalesce(columns: _*))
  }

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandler]] is structured.
   * This function provides the information on the structure of single error column
   *
   * @return - `BooleanType`, as all what is needed is a true flag if error was present
   */
  val errorColumnType: DataType = BooleanType

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandler]] is structured.
   * This function describes what is the type of the column attached (if it didn't exists before) to the [[org.apache.spark.sql.DataFrame DataFrame]]
   *
   * @return  - `None` since no error-related column is added
   */
  val dataFrameColumnType: Option[DataType] = None
}
