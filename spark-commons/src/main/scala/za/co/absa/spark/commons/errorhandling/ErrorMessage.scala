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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import za.co.absa.spark.commons.errorhandling.ErrorMessage.Mapping
import za.co.absa.spark.commons.errorhandling.types._

/**
 * Case class to represent an error message
 *
 * @param errType - Type or source of the error
 * @param errCode - Internal error code
 * @param errMsg - Textual description of the error
 * @param errCol - The name of the column where the error occurred
 * @param rawValues - Sequence of raw values (which are the potential culprits of the error)
 * @param additionInfo - Sequence of Mappings i.e Mapping Table Column -> Equivalent Mapped Dataset column
 */
case class ErrorMessage(
                         errType: ErrType,
                         errCode: ErrCode,
                         errMsg: ErrMsg,
                         errCol: ErrCol,
                         rawValues: RawValues,
                         additionInfo: AdditionalInfo = None
                       )

object ErrorMessage {
  //TODO probably not needed
  case class Mapping(
                      mappingTableColumn: String,
                      mappedDatasetColumn: String
                    )

  val errorColumnName = "errCol"
  def errorColSchema(implicit spark: SparkSession): StructType = {
    import spark.implicits._
    spark.emptyDataset[ErrorMessage].schema
  }
}

