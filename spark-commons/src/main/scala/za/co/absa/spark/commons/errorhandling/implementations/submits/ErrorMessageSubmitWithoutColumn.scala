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

package za.co.absa.spark.commons.errorhandling.implementations.submits

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.typedLit
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.implementations.submits.ErrorMessageSubmitWithoutColumn.emptyErrorColsAndValues
import za.co.absa.spark.commons.errorhandling.types._

/**
 * [[za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit ErrorMessageSubmit]] subclass to represent an error not
 * bound to any particular column.
 * @param errType - error type
 * @param errCode - error code
 * @param errMessage - error message
 * @param additionalInfo - optional additional info in form of JSON
 * @group Error Handling
 * @since 0.6.0
 */
class ErrorMessageSubmitWithoutColumn(
                                       val errType: ColumnOrValue[ErrType],
                                       val errCode: ColumnOrValue[ErrCode],
                                       val errMessage: ColumnOrValue[ErrMsg],
                                       override val additionalInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
                                     ) extends ErrorMessageSubmit {

  val errColsAndValues: ColumnOrValue[ErrColsAndValues] =  ColumnOrValue(ErrorMessageSubmitWithoutColumn.emptyErrColsAndValues)
}

object ErrorMessageSubmitWithoutColumn {
  private val emptyErrorColsAndValues: ErrColsAndValues = Map.empty

  val emptyErrColsAndValues: Column = typedLit(emptyErrorColsAndValues)

  /**
   * Convenient apply function
   * @param errType - error type
   * @param errCode - error code
   * @param errMessage - error message
   * @param additionalInfo - optional additional info in form of JSON
   * @return - instance of [[ErrorMessageSubmitWithoutColumn]]
   * @group Error Handling
   * @since 0.6.0
   */
  def apply(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, additionalInfo: AdditionalInfo = None): ErrorMessageSubmitWithoutColumn = {
    new ErrorMessageSubmitWithoutColumn(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      ColumnOrValue.withOption(additionalInfo)
    )
  }
}
