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

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class ErrorMessageTest extends AnyFunSuite with SparkTestBase {
  test("errorColSchema returns the expected structure"){
    val expected = StructType(Seq(
      StructField("errType", StringType, nullable = true),
      StructField("errCode", LongType, nullable = false),
      StructField("errMsg", StringType, nullable = true),
      StructField("errCol", StringType, nullable = true),
      StructField("rawValues", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("additionInfo", StringType, nullable = true)
    ))

    val result = ErrorMessage.errorColSchema
    assert(result == expected)
  }
}
