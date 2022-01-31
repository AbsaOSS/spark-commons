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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.ColumnImplicits.ColumnEnhancements

class ColumnImplicitsTest extends AnyFunSuite{

  private val column: Column = lit("abcdefgh")

  test("zeroBasedSubstr with startPos") {
    assertResult("cdefgh")(column.zeroBasedSubstr(2).expr.eval().toString)
    assertResult("gh")(column.zeroBasedSubstr(-2).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue).expr.eval().toString)
    assertResult("abcdefgh")(column.zeroBasedSubstr(Int.MinValue).expr.eval().toString)
  }

  test("zeroBasedSubstr with startPos and len") {
    assertResult("cde")(column.zeroBasedSubstr(2, 3).expr.eval().toString)
    assertResult("gh")(column.zeroBasedSubstr(-2, 7).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue, 1).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue, -3).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(4, -3).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MinValue,2).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MinValue,-3).expr.eval().toString)
  }

}
