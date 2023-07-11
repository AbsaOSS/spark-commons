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

package za.co.absa.spark.commons.errorhandler

import za.co.absa.spark.commons.errorhandler.types._

/**
 * Case class to represent an error message
 *
 * @param errType - Type or source of the error
 * @param errCode - Internal error code
 * @param errMsg - Textual description of the error
 * @param errColsAndValues - The names of the columns where the error occurred and their raw values (which are the
 *                         potential culprits of the error)
 * @param additionInfo - any optional additional information in the form of a JSON string
 */
case class ErrorMessage(
                         errType: ErrType,
                         errCode: ErrCode,
                         errMsg: ErrMsg,
                         errColsAndValues: ErrColsAndValues,
                         additionInfo: AdditionalInfo = None
                       )
