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

package za.co.absa.spark.commons.errorhandler.implementations.submits

import za.co.absa.spark.commons.errorhandler.types._

/**
 * [[za.co.absa.spark.commons.errorhandler.ErrorMessageSubmit ErrorMessageSubmit]] subclass to represent an error bound to exactly one
 * column.
 *
 * @param errType - error type
 * @param errCode - error code
 * @param errMessage - error message
 * @param errSourceColName - the name of the column the error was detected on
 * @param additionalInfo - optional additional info in form of JSON
 * @group Error Handling
 * @since 0.6.0
 */
class ErrorMessageSubmitOnColumn (
                                   errType: ColumnOrValue[ErrType],
                                   errCode: ColumnOrValue[ErrCode],
                                   errMessage: ColumnOrValue[ErrMsg],
                                   errSourceColName: ErrSourceColName,
                                   override val additionalInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
                                 ) extends ErrorMessageSubmitOnMoreColumns(errType, errCode, errMessage, Set(errSourceColName), additionalInfo) {

}

object ErrorMessageSubmitOnColumn {

  /**
   * Convenient apply function
   * @param errType - error type
   * @param errCode - error code
   * @param errMessage - error message
   * @param errSourceColName - the name of the column the error was detected on
   * @param additionalInfo - optional additional info in form of JSON
   * @return - instance of [[ErrorMessageSubmitOnColumn]]
   * @group Error Handling
   * @since 0.6.0
   */
  def apply(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errSourceColName: ErrSourceColName, additionalInfo: AdditionalInfo= None): ErrorMessageSubmitOnColumn = {
    new ErrorMessageSubmitOnColumn(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      errSourceColName,
      ColumnOrValue.withOption(additionalInfo)
    )
  }
}
