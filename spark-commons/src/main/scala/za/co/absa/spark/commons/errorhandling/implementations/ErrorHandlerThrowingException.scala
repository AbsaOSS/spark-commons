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

import org.apache.spark.sql.functions.{coalesce, udf, when}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DataType
import za.co.absa.spark.commons.errorhandling.partials.TransformIntoErrorMessage.FieldNames._
import za.co.absa.spark.commons.errorhandling.{ErrorHandling, ErrorMessage}
import za.co.absa.spark.commons.errorhandling.partials.TransformIntoErrorMessage
import za.co.absa.spark.commons.sql.functions.null_col

object ErrorHandlerThrowingException extends ErrorHandling with TransformIntoErrorMessage {
  case class ErrorHandlerException(error: ErrorMessage) extends Exception(error.errMsg)

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
   * @param errCols   - the list of error columns to apply
   * @return - data frame with the error columns applied (aggregated and attached or done otherwise)
   */
  override protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
    val errorColumn= coalesce(errCols: _*)
    val errColName = dataFrame.schema.getClosestUniqueName("_1")
    dataFrame.withColumn(errColName, when(errorColumn.isNull, null_col).otherwise(throwError(
      //the values have to be sent decomposed, sending the whoel struct field fails the udf function
      errorColumn.getField(errType),
      errorColumn.getField(errCode),
      errorColumn.getField(errMsg),
      errorColumn.getField(errColsAndValues),
      errorColumn.getField(additionInfo)
    ))).drop(errColName) // to make the column effective computation it has to be added, then it can be dropped
  }

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandling]] is structured.
   * This function describes what is the type of the column attached (if it didn't exists before) to the [[org.apache.spark.sql.DataFrame DataFrame]]
   *
   * @return - the DataType of the column containing the error info that is attached to the [[org.apache.spark.sql.DataFrame DataFrame]].
   */
  override def dataFrameColumnType: Option[DataType] = None

  private val throwError = udf({
    (errType: String, errCode: Long, errMsg: String, errColsAndValues: Map[String, String], additionInfo: String) => {
      val error = ErrorMessage(errType, errCode, errMsg, errColsAndValues, Option(additionInfo))
      throw ErrorHandlerException(error)

    }
  })

}
