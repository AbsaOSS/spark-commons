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

import org.apache.spark.sql.functions.{lit, map}
import org.apache.spark.sql.types.StringType
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.implementations.submits.ErrorMessageSubmitJustErrorValue.noColumnKey
import za.co.absa.spark.commons.errorhandling.types._


/**
 * [[za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit ErrorMessageSubmit]] subclass to represent an error not
 * bound to a particular column but still having a value that caused the error.
 * @param errType - error type
 * @param errCode - error code
 * @param errMessage - error message
 * @param errValue - the value that caused the error
 * @param additionalInfo - optional additional info in form of JSON
 * @group Error Handling
 * @since 0.6.0
 */
class ErrorMessageSubmitJustErrorValue(
                                        val errType: ColumnOrValue[ErrType],
                                        val errCode: ColumnOrValue[ErrCode],
                                        val errMessage: ColumnOrValue[ErrMsg],
                                        errValue: ColumnOrValue[ErrValue],
                                        override val additionalInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
                                      ) extends ErrorMessageSubmit {
  val errColsAndValues: ColumnOrValue[ErrColsAndValues] = ColumnOrValue(map(lit(noColumnKey), errValue.column.cast(StringType)))
}

object ErrorMessageSubmitJustErrorValue {
  val noColumnKey: ErrSourceColName = ""

  /**
   * Convenient apply function
   * @param errType - error type
   * @param errCode - error code
   * @param errMessage - error message
   * @param errValue - the value that caused the error
   * @param additionalInfo - optional additional info in form of JSON
   * @return - instance of [[ErrorMessageSubmitJustErrorValue]]
   * @group Error Handling
   * @since 0.6.0
   */
  def apply(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errValue: String, additionalInfo: AdditionalInfo = None): ErrorMessageSubmitJustErrorValue = {
    new ErrorMessageSubmitJustErrorValue(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      ColumnOrValue.withValue(errValue),
      ColumnOrValue.withOption(additionalInfo)
    )
  }
}
