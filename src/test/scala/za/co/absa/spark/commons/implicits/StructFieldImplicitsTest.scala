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

package za.co.absa.spark.commons.implicits

import org.apache.spark.sql.types.{Metadata, StringType, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldMetadataEnhancement

class StructFieldImplicitsTest extends AnyFunSuite {

  def fieldWith(value123: String) = {
    val value1 = s"""{ \"a\" : ${value123} }"""
    StructField("uu", StringType, true, Metadata.fromJson(value1))
  }

  test("getMetadataString") {
    assertResult(Some(""))(fieldWith("\"\"").metadata.getOptString("a"))
    assertResult(None)(fieldWith("123").metadata.getOptString("a"))
    assertResult(Some("ffbfg"))(fieldWith("\"ffbfg\"").metadata.getOptString("a"))
    assertResult(Some(null))(fieldWith("null").metadata.getOptString("a"))
  }

  test("getOptChar") {
    assertResult(None)(fieldWith("\"\"").metadata.getOptChar("a"))
    assertResult(None)(fieldWith("123").metadata.getOptChar("a"))
    assertResult(Some('g'))(fieldWith("\"g\"").metadata.getOptChar("a"))
    assertResult(None)(fieldWith("\"abc\"").metadata.getOptChar("a"))
    assertResult(None)(fieldWith("null").metadata.getOptChar("a"))
  }

  test("getStringAsBoolean") {
    assertResult(None)(fieldWith("\"\"").metadata.getOptStringAsBoolean("a"))
    assertResult(None)(fieldWith("123").metadata.getOptStringAsBoolean("a"))
    assertResult(Some(true))(fieldWith("\"true\"").metadata.getOptStringAsBoolean("a"))
    assertResult(Some(false))(fieldWith("\"false\"").metadata.getOptStringAsBoolean("a"))
    assertResult(None)(fieldWith("false").metadata.getOptStringAsBoolean("a"))
    assertResult(None)(fieldWith("true").metadata.getOptStringAsBoolean("a"))
    assertResult(None)(fieldWith("null").metadata.getOptStringAsBoolean("a"))
  }

  test("hastMetadataKey") {
    assertResult(true)(fieldWith("\"\"").metadata.hasKey("a"))
    assertResult(false)(fieldWith("123").metadata.hasKey("b"))
    assertResult(true)(fieldWith("\"hvh\"").metadata.hasKey("a"))
    assertResult(true)(fieldWith("null").metadata.hasKey("a"))
  }

}
