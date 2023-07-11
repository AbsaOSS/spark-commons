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

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.errorhandling.{ErrorHandlerFilteringErrorRows, ErrorHandler, ErrorMessage}
import za.co.absa.spark.commons.errorhandling.implementations.submits.ErrorMessageSubmitOnColumn
import za.co.absa.spark.commons.test.SparkTestBase
import ErrorHandlerThrowingException.ErrorHandlerException
import org.apache.spark.SparkException

class ErrorHandlerThrowingExceptionTest extends AnyFunSuite with SparkTestBase {
  import spark.implicits._
  import ErrorHandlerFilteringErrorRows._
  private implicit val errorHandlerThrowingException: ErrorHandler = ErrorHandlerThrowingException

  private val col1Name = "Col1"
  private val col2Name = "Col2"

  private val errorMessageSubmit = ErrorMessageSubmitOnColumn("not_happening", 7, "This should not happen", col1Name)

  private val srcDf = Seq(
    (None, ""),
    (Some(1), "a"),
    (Some(2), "bb"),
    (Some(3), "ccc")
  ).toDF(col1Name, col2Name)

  test("No error, checking schema") {
    val newDf = srcDf.putError(col(col1Name) > 4)(errorMessageSubmit)
    newDf.cache()
    assert(errorHandlerThrowingException.dataFrameColumnType.isEmpty)
    assert(newDf.schema == srcDf.schema)
  }

  test("Throw error") {
    val newDf = srcDf.putError(col(col1Name) > 2)(errorMessageSubmit)
    val e = intercept[SparkException] {
      newDf.cache()
    }
    assert(e.getCause == ErrorHandlerException(ErrorMessage("not_happening", 7, "This should not happen", Map(col1Name -> "3"))))
  }
}
