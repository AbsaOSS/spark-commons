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

package za.co.absa.spark.commons.errorhandling

import org.apache.spark.sql.Column

package object types {
  type ErrColName = String

  type ErrType = String
  type ErrCode = Long // was string
  type ErrMsg = String
  type ErrCols = Seq[ErrColName]
  type RawValues = Seq[String]
  type AdditionalInfo = Option[String] // actually a JSON
  //mapping is missing, should be part of AdditionalInfo, as being very specific

  //This is to ensure some level of type-safety
  final case class ErrorColumn(column: Column) extends AnyVal
}
