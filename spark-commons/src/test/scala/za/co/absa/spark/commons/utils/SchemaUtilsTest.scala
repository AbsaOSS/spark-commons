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

package za.co.absa.spark.commons.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.utils.SchemaUtils._

class SchemaUtilsTest extends AnyFunSuite with Matchers {
  // scalastyle:off magic.number

  test("Test isCommonSubPath()") {
    assert (isCommonSubPath())
    assert (isCommonSubPath("a"))
    assert (isCommonSubPath("a.b.c.d.e.f", "a.b.c.d", "a.b.c", "a.b", "a"))
    assert (!isCommonSubPath("a.b.c.d.e.f", "a.b.c.x", "a.b.c", "a.b", "a"))
  }

  test("Test getParentPath") {
    assertResult(getParentPath("a.b.c.d.e"))("a.b.c.d")
    assertResult(getParentPath("a"))("")
    assertResult(getParentPath("a.bcd"))("a")
    assertResult(getParentPath(""))("")
  }

  test("Test splitPath") {
    assertResult(List("a", "b", "c", "d", "e"))(splitPath("a.b.c.d.e"))
    assertResult(List("a"))(splitPath("a"))
    assertResult(List("a", "bcd"))(splitPath("a.bcd"))
    assertResult(List("a", "bcd"))(splitPath("a.bcd."))
    assertResult(List("", "a", "bcd"))(splitPath(".a.bcd"))
    assertResult(List.empty[String])(splitPath(""))
    assertResult(List.empty[String])(splitPath("."))
  }

}
