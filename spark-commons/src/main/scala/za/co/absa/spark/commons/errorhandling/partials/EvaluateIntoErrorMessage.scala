/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.commons.errorhandling.partials

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.struct
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit

trait EvaluateIntoErrorMessage {
  protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
    struct(
      errorMessageSubmit.errType.column as "errType",
      errorMessageSubmit.errCode.column as "errCode",
      errorMessageSubmit.errMsg.column as "errMsg",
      errorMessageSubmit.errCol.column as "errCol",
      errorMessageSubmit.rawValues.column as "rawValues",
      errorMessageSubmit.additionInfo.column as "additionInfo"
    )
  }

}
