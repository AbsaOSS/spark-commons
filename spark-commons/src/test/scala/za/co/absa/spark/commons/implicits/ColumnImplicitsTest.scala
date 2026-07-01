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
import za.co.absa.spark.commons.utils.ColUtils

class ColumnImplicitsTest extends AnyFunSuite {

  private val column: Column = lit("abcdefgh")

  test("zeroBasedSubstr with startPos") {
    assertResult("cdefgh")(ColUtils.col2Expr(column.zeroBasedSubstr(2)).eval().toString)
    assertResult("gh")(ColUtils.col2Expr(column.zeroBasedSubstr(-2)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MaxValue)).eval().toString)
    assertResult("abcdefgh")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MinValue)).eval().toString)
  }

  test("zeroBasedSubstr with startPos and len") {
    assertResult("cde")(ColUtils.col2Expr(column.zeroBasedSubstr(2, 3)).eval().toString)
    assertResult("gh")(ColUtils.col2Expr(column.zeroBasedSubstr(-2, 7)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MaxValue, 1)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MaxValue, -3)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(4, -3)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MinValue,2)).eval().toString)
    assertResult("")(ColUtils.col2Expr(column.zeroBasedSubstr(Int.MinValue,-3)).eval().toString)
  }

}
