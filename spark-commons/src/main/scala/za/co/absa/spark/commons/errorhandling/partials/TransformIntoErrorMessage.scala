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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.partials.TransformIntoErrorMessage.FieldNames._

/**
 * Trait offers a presumably very common implementation of [[za.co.absa.spark.commons.errorhandling.ErrorHandling.transformErrorSubmitToColumn ErrorHandling.transformErrorSubmitToColumn()]],
 * where the error is transformed into the struct of [[za.co.absa.spark.commons.errorhandling.ErrorMessage ErrorMessage]].
 * @group Error Handling
 * @since 0.6.0
 */
trait TransformIntoErrorMessage {
  protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column = {
    struct(
      errorMessageSubmit.errType.column as errType,
      errorMessageSubmit.errCode.column as errCode,
      errorMessageSubmit.errMessage.column as errMsg,
      errorMessageSubmit.errColsAndValues.column as errColsAndValues,
      errorMessageSubmit.additionalInfo.column as additionInfo
    )
  }

  /**
   * Describes what is the type of the error column
   * @return StructType of DataType object
   */
  def errorColumnType: DataType = {
    StructType(Seq(
      StructField(errType, StringType, nullable = false),
      StructField(errCode, LongType, nullable = false),
      StructField(errMsg, StringType, nullable = false),
      StructField(errColsAndValues, MapType(StringType, StringType, valueContainsNull = true), nullable = false),
      StructField(additionInfo, StringType, nullable = true)
    ))
  }
}

object TransformIntoErrorMessage {
  object FieldNames {
    val errType = "errType"
    val errCode = "errCode"
    val errMsg = "errMsg"
    val errColsAndValues = "errColsAndValues"
    val additionInfo = "additionInfo"
    val errSourceCols = "errSourceCols"
    val errValues = "errValues"
  }

}
