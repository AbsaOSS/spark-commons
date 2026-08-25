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
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.ColumnImplicits.ColumnEnhancements
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.utils.ColUtils

class ColumnImplicitsTest extends AnyFunSuite with SparkTestBase {

  private val column: Column = lit("abcdefgh")

  test("zeroBasedSubstr with startPos") {
    assertResult("cdefgh")(evalString(column.zeroBasedSubstr(2)))
    assertResult("gh")(evalString(column.zeroBasedSubstr(-2)))
    assertResult("")(evalString(column.zeroBasedSubstr(Int.MaxValue)))
    assertResult("abcdefgh")(evalString(column.zeroBasedSubstr(Int.MinValue)))
  }

  test("zeroBasedSubstr with startPos and len") {
    assertResult("cde")(evalString(column.zeroBasedSubstr(2, 3)).toString)
    assertResult("gh")(evalString(column.zeroBasedSubstr(-2, 7)).toString)
    assertResult("")(evalString(column.zeroBasedSubstr(Int.MaxValue, 1)).toString)
    assertResult("")(evalString(column.zeroBasedSubstr(Int.MaxValue, -3)).toString)
    assertResult("")(evalString(column.zeroBasedSubstr(4, -3)).toString)
    assertResult("")(evalString(column.zeroBasedSubstr(Int.MinValue,2)).toString)
    assertResult("")(evalString(column.zeroBasedSubstr(Int.MinValue,-3)).toString)
  }

  private def evalString(column: Column): String =
    spark.range(1).select(column.as("value")).head().getString(0)

  private def evalBoolean(column: Column): Boolean =
    spark.range(1).select(column.as("value")).head().getBoolean(0)

  private def evalIsNull(column: Column): Boolean = {
    val row = spark.range(1).select(column.as("value")).head()
    row.isNullAt(0)
  }

  test("isInfinite returns true for positive infinity") {
    assertResult(true)(evalBoolean(lit(Double.PositiveInfinity).isInfinite))
  }

  test("isInfinite returns true for negative infinity") {
    assertResult(true)(evalBoolean(lit(Double.NegativeInfinity).isInfinite))
  }

  test("isInfinite returns false for finite values") {
    assertResult(false)(evalBoolean(lit(0.0).isInfinite))
    assertResult(false)(evalBoolean(lit(1.5).isInfinite))
    assertResult(false)(evalBoolean(lit(Double.MaxValue).isInfinite))
    assertResult(false)(evalBoolean(lit(Double.MinValue).isInfinite))
  }

  test("isInfinite returns false for zero") {
    assertResult(false)(evalBoolean(lit(0.0).isInfinite))
    assertResult(false)(evalBoolean(lit(-0.0).isInfinite))
  }

  test("isInfinite with DataFrame column") {
    import spark.implicits._
    val df = Seq(
      (Double.PositiveInfinity, "pos_inf"),
      (Double.NegativeInfinity, "neg_inf"),
      (1.0, "finite"),
      (0.0, "zero")
    ).toDF("value", "label")

    val resultDf = df.withColumn("is_inf", col("value").isInfinite)

    assertResult(true)(resultDf.filter("label = 'pos_inf'").select("is_inf").head().getBoolean(0))
    assertResult(true)(resultDf.filter("label = 'neg_inf'").select("is_inf").head().getBoolean(0))
    assertResult(false)(resultDf.filter("label = 'finite'").select("is_inf").head().getBoolean(0))
    assertResult(false)(resultDf.filter("label = 'zero'").select("is_inf").head().getBoolean(0))
  }

  test("zeroBasedSubstr with empty string") {
    assertResult("")(evalString(lit("").zeroBasedSubstr(0)))
    assertResult("")(evalString(lit("").zeroBasedSubstr(0, 5)))
    assertResult("")(evalString(lit("").zeroBasedSubstr(-1)))
  }

  test("zeroBasedSubstr with null input") {
    assert(evalIsNull(lit(null: String).zeroBasedSubstr(0)))
    assert(evalIsNull(lit(null: String).zeroBasedSubstr(0, 5)))
    assert(evalIsNull(lit(null: String).zeroBasedSubstr(-1)))
  }

  test("zeroBasedSubstr with zero length") {
    assertResult("")(evalString(column.zeroBasedSubstr(0, 0)))
  }
}
