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

import org.apache.spark.sql.functions.{lit, map}
import org.apache.spark.sql.types.StringType
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.implementations.ErrorMessageSubmitJustErrorValue.noColumnKey
import za.co.absa.spark.commons.errorhandling.types._

case class ErrorMessageSubmitJustErrorValue(
                                             errType: ColumnOrValue[ErrType],
                                             errCode: ColumnOrValue[ErrCode],
                                             errMsg: ColumnOrValue[ErrMsg],
                                             errValue: ColumnOrValue[String],
                                             override val additionInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
                                           ) extends ErrorMessageSubmit {
  val errColsAndValues: ColumnOrValue[ErrColsAndValues] = ColumnOrValue(map(lit(noColumnKey), errValue.column.cast(StringType)))
}

object ErrorMessageSubmitJustErrorValue {
  val noColumnKey: ErrSourceColName = ""

  def apply(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errValue: String): ErrorMessageSubmitJustErrorValue = {
    new ErrorMessageSubmitJustErrorValue(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      ColumnOrValue.withValue(errValue),
      ColumnOrValue.asEmpty
    )
  }

  def apply(errType: ErrType, errCode: ErrCode, errMessage: ErrMsg, errValue: String, additionalInfo: AdditionalInfo): ErrorMessageSubmitJustErrorValue = {
    new ErrorMessageSubmitJustErrorValue(
      ColumnOrValue.withValue(errType),
      ColumnOrValue.withValue(errCode),
      ColumnOrValue.withValue(errMessage),
      ColumnOrValue.withValue(errValue),
      ColumnOrValue.withOption(additionalInfo)
    )
  }
}
