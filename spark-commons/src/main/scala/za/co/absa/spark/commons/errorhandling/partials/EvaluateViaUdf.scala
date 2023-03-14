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

package za.co.absa.spark.commons.errorhandling.partials

import org.apache.spark.sql.{Column, SparkSession}
import za.co.absa.spark.commons.adapters.CallUdfAdapter
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.partials.EvaluateViaUdf.ErrorMessageFunction
import za.co.absa.spark.commons.errorhandling.types._

trait EvaluateViaUdf[T] extends CallUdfAdapter{
  def evaluationUdfName: String
  protected def evaluationUdf: ErrorMessageFunction[T]
  def register(sparkToRegisterTo: SparkSession): Unit // TODO refactor when #82 has been implemented

  protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
   call_udf(evaluationUdfName,
     errorMessageSubmit.errType.column,
     errorMessageSubmit.errCode.column,
     errorMessageSubmit.errMsg.column,
     errorMessageSubmit.errColsAndValues.column,
     errorMessageSubmit.additionInfo.column
   )
  }
}

object EvaluateViaUdf {
  type ErrorMessageFunction[T] = (ErrType, ErrCode, ErrMsg, ErrColsAndValues, AdditionalInfo) => T //TODO needed?
}
