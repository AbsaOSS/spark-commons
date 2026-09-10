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

package za.co.absa.spark.commons.adapters

import org.apache.spark.sql.functions.{col, lit, udf}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class CallUdfAdapterTest extends AnyFunSuite with SparkTestBase with CallUdfAdapter {

  import spark.implicits._

  private val testDf = Seq(
    ("hello", 1),
    ("world", 2),
    ("test", 3)
  ).toDF("str_col", "num_col")

  private def evalString(column: org.apache.spark.sql.Column): String =
    spark.range(1).select(column.as("value")).head().getString(0)

  private def evalInt(column: org.apache.spark.sql.Column): Int =
    spark.range(1).select(column.as("value")).head().getInt(0)

  private def evalBoolean(column: org.apache.spark.sql.Column): Boolean =
    spark.range(1).select(column.as("value")).head().getBoolean(0)

  private def evalResult(df: org.apache.spark.sql.DataFrame, colName: String): String =
    df.select(colName).head().getString(0)

  test("call_udf with a single column argument") {
    spark.udf.register("my_upper", (s: String) => s.toUpperCase)

    val result = call_udf("my_upper", lit("hello"))

    assertResult("HELLO")(evalString(result))
  }

  test("call_udf with multiple column arguments") {
    spark.udf.register("my_concat", (a: String, b: String) => s"$a$b")

    val result = call_udf("my_concat", lit("hello"), lit(" world"))

    assertResult("hello world")(evalString(result))
  }

  test("call_udf with DataFrame columns") {
    spark.udf.register("my_upper", (s: String) => s.toUpperCase)

    val resultDf = testDf.withColumn("upper_str", call_udf("my_upper", col("str_col")))

    assertResult("HELLO")(evalResult(resultDf, "upper_str"))
  }

  test("call_udf with multiple DataFrame columns") {
    spark.udf.register("my_sum", (a: Int, b: Int) => a + b)

    val resultDf = testDf.withColumn("sum", call_udf("my_sum", col("num_col"), lit(10)))

    assertResult(11)(resultDf.select("sum").head().getInt(0))
  }

  test("call_udf with a boolean-returning UDF") {
    spark.udf.register("my_is_positive", (n: Int) => n > 0)

    val result = call_udf("my_is_positive", lit(5))

    assertResult(true)(evalBoolean(result))
  }

  test("call_udf with negative value for boolean UDF") {
    spark.udf.register("my_is_positive", (n: Int) => n > 0)

    val result = call_udf("my_is_positive", lit(-3))

    assertResult(false)(evalBoolean(result))
  }

  test("call_udf throws when UDF is not registered") {
    assertThrows[Throwable] {
      val result = call_udf("nonexistent_udf", lit("hello"))
      evalString(result)
    }
  }

  test("call_udf with null input") {
    spark.udf.register("my_upper", (s: String) => if (s == null) null else s.toUpperCase)

    val result = call_udf("my_upper", lit(null))

    val row = spark.range(1).select(result.as("value")).head()
    assert(row.isNullAt(0))
  }
}
