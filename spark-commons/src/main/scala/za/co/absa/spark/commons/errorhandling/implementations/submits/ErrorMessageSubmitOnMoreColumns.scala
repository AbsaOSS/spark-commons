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

import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.types._

/**
 * [[za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit ErrorMessageSubmit]] subclass to represent an error bound
 * to multiple columns.
 * @param errType - error type
 * @param errCode - error code
 * @param errMessage - error message
 * @param errSourceColNames - the name of the columns the error was detected on
 * @param additionalInfo - optional additional info in form of JSON
 * @group Error Handling
 * @since 0.6.0
 */
class ErrorMessageSubmitOnMoreColumns(
                                       val errType: ColumnOrValue[ErrType],
                                       val errCode: ColumnOrValue[ErrCode],
                                       val errMessage: ColumnOrValue[ErrMsg],
                                       errSourceColNames: Set[ErrSourceColName],
                                       override val additionalInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
                                     ) extends ErrorMessageSubmit {
  val errColsAndValues: ColumnOrValue[ErrColsAndValues] = ColumnOrValue.asMapOfStringColumns(errSourceColNames)
}

object ErrorMessageSubmitOnMoreColumns {
  /**
   * Convenient apply function
   * @param errType - error type
   * @param errCode - error code
   * @param errMessage - error message
   * @param errSourceColNames - the name of the columns the error was detected on
   * @param additionalInfo - optional additional info in form of JSON
   * @return - instance of [[ErrorMessageSubmitOnMoreColumns]]
   * @group Error Handling
   * @since 0.6.0
   */
  def apply(errType: ErrType,
            errCode: ErrCode,
            errMessage: ErrMsg,
            errSourceColNames: Set[ErrSourceColName],

            additionalInfo: AdditionalInfo= None): ErrorMessageSubmitOnMoreColumns = {
    new ErrorMessageSubmitOnMoreColumns(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      errSourceColNames,
      ColumnOrValue.withOption(additionalInfo)
    )
  }
}
