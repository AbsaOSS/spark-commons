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

import org.apache.spark.sql.functions.{array, lit, map}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandler.types.{AdditionalInfo, ColumnOrValueForm, ErrColsAndValues}
import za.co.absa.spark.commons.sql.functions.null_col

class ErrorMessageSubmitJustErrorValueTest extends AnyFunSuite {
  test("Apply function properly hands over data without additional info") {
    val errType = "Test error"
    val errCode = 201L
    val errMsg = "This is a test error"
    val errValue = "This was wrong"

    val result = ErrorMessageSubmitJustErrorValue(errType, errCode, errMsg, errValue)

    val expectedErrType = ColumnOrValueForm(lit(errType), Set.empty, Some(errType))
    val expectedErrCode = ColumnOrValueForm(lit(errCode), Set.empty, Some(errCode))
    val expectedErrMsg = ColumnOrValueForm(lit(errMsg), Set.empty, Some(errMsg))
    val column = map(lit(""), lit(errValue).cast(StringType))
    val expectedErrValuesCol = ColumnOrValueForm[ErrColsAndValues](column, Set.empty, None)
    val expectedAdditionalInfo = ColumnOrValueForm[AdditionalInfo](null_col(StringType), Set.empty, None)

    expectedErrType assertTo result.errType
    expectedErrCode assertTo result.errCode
    expectedErrMsg assertTo result.errMessage
    expectedErrValuesCol assertTo result.errColsAndValues
    expectedAdditionalInfo assertTo result.additionalInfo
  }

  test("Apply function properly hands over data with additional info") {
    val errType = "Test error"
    val errCode = 201L
    val errMsg = "This is a test error"
    val errValue = "This was wrong"
    val additionalInfo = "{}"

    val result = ErrorMessageSubmitJustErrorValue(errType, errCode, errMsg, errValue, Some(additionalInfo))

    val expectedErrType = ColumnOrValueForm(lit(errType), Set.empty, Some(errType))
    val expectedErrCode = ColumnOrValueForm(lit(errCode), Set.empty, Some(errCode))
    val expectedErrMsg = ColumnOrValueForm(lit(errMsg), Set.empty, Some(errMsg))
    val column = map(lit(""), lit(errValue).cast(StringType))
    val expectedErrValuesCol = ColumnOrValueForm[ErrColsAndValues](column, Set.empty, None)
    val expectedAdditionalInfo = ColumnOrValueForm[AdditionalInfo](lit(additionalInfo), Set.empty, Some(Some(additionalInfo)))

    expectedErrType assertTo result.errType
    expectedErrCode assertTo result.errCode
    expectedErrMsg assertTo result.errMessage
    expectedErrValuesCol assertTo result.errColsAndValues
    expectedAdditionalInfo assertTo result.additionalInfo

  }
}
