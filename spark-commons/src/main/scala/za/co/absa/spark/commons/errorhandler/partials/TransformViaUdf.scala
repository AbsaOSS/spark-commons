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

package za.co.absa.spark.commons.errorhandler.partials

import org.apache.spark.sql.{Column, SparkSession}
import za.co.absa.spark.commons.OncePerSparkSession
import za.co.absa.spark.commons.adapters.CallUdfAdapter
import za.co.absa.spark.commons.errorhandler.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandler.partials.TransformViaUdf.ErrorMessageFunction
import za.co.absa.spark.commons.errorhandler.types._

trait TransformViaUdf[T] extends OncePerSparkSession with CallUdfAdapter {
  def transformationUdfName: String
  protected def transformationUdf: ErrorMessageFunction[T]

  protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column = {
   call_udf(transformationUdfName,
            errorMessageSubmit.errType.column,
            errorMessageSubmit.errCode.column,
            errorMessageSubmit.errMessage.column,
            errorMessageSubmit.errColsAndValues.column,
            errorMessageSubmit.additionalInfo.column)
  }
}

object TransformViaUdf {
  type ErrorMessageFunction[T] = (ErrType, ErrCode, ErrMsg, ErrColsAndValues, AdditionalInfo) => T //TODO needed?
}
