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
 * Trait collecting error definition in a format usable during Spark data processing
 * @group Error Handling
 * @since 0.6.0
 */
trait ErrorMessageSubmit {
  def errType: ColumnOrValue[ErrType]
  def errCode: ColumnOrValue[ErrCode]
  def errMessage: ColumnOrValue[ErrMsg]
  def errColsAndValues: ColumnOrValue[ErrColsAndValues]
  def additionalInfo: ColumnOrValue[AdditionalInfo] = ColumnOrValue.asEmpty
}
