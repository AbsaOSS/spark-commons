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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

class DataFrameImplicitsSuite extends AnyFunSuite with SparkTestBase  {
  import spark.implicits._

  private val columnName = "data"
  private val inputDataSeq = Seq(
    "0123456789012345678901234",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z"
  )
  private val inputData = inputDataSeq.toDF(columnName)

  import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

  private def getDummyDataFrame: DataFrame = {
    import spark.implicits._

    Seq(1, 1, 1, 2, 1).toDF("value")
  }

  private def cellText(text: String, width: Int, leftAlign: Boolean): String = {
    val pad = " " * (width - text.length)
    if (leftAlign) {
      text + pad
    } else {
      pad + text
    }
  }

  private def line(width: Int): String = {
    "+" + "-" * width + "+"
  }

  private def header(width: Int, leftAlign: Boolean): String = {
    val lineStr = line(width)
    val title = cellText(columnName, width, leftAlign)
    s"$lineStr\n|$title|\n$lineStr"
  }

  private def cell(text: String, width: Int, leftAlign: Boolean): String = {
    val inner = if (text.length > width) {
      text.substring(0, width - 3) + "..."
    } else {
      cellText(text, width, leftAlign)
    }
    s"|$inner|"
  }

  private def inputDataToString(width: Int, leftAlign: Boolean, limit: Option[Int] = Option(20)): String = {
    val (extraLine, seq) = limit match {
      case Some(n) =>
        val line =  if (inputDataSeq.length > n) {
          s"only showing top $n rows\n"
        } else {
          ""
        }
        (line, inputDataSeq.take(n))
      case None    =>
        ("", inputDataSeq)
    }
    seq.foldLeft(header(width, leftAlign)) { (acc, item) =>
      acc + "\n" + cell(item, width, leftAlign)
    } + "\n" + line(width) + s"\n$extraLine\n"
  }

  test("Like show()") {
    val result = inputData.dataAsString()
    val leftAlign = false
    val cellWidth = 20
    val expected = inputDataToString(cellWidth, leftAlign)

    assert(result == expected)
  }

  test("Like show(false)") {
    val result = inputData.dataAsString(false)
    val leftAlign = true
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign)

    assert(result == expected)
  }

  test("Like show(3, true)") {
    val result = inputData.dataAsString(3, true)
    val leftAlign = false
    val cellWidth = 20
    val expected = inputDataToString(cellWidth, leftAlign, Option(3))

    assert(result == expected)
  }

  test("Like show(30, false)") {
    val result = inputData.dataAsString(30, false)
    val leftAlign = true
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign, Option(30))

    assert(result == expected)
  }


  test("Like show(10, 10)") {
    val result = inputData.dataAsString(10, 10)
    val leftAlign = false
    val cellWidth = 10
    val expected = inputDataToString(cellWidth, leftAlign, Option(10))

    assert(result == expected)
  }

  test("Like show(50, 50, false)") {
    val result = inputData.dataAsString(50, 50, false)
    val leftAlign = false
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign, Option(50))

    assert(result == expected)
  }

  test("Test withColumnIfNotExist() when the column does not exist") {
    val expectedOutput =
      """+-----+---+
        ||value|foo|
        |+-----+---+
        ||1    |1  |
        ||1    |1  |
        ||1    |1  |
        ||2    |1  |
        ||1    |1  |
        |+-----+---+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = dfIn.withColumnIfDoesNotExist("foo", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfOut.schema.length == 2)
    assert(dfOut.schema.head.name == "value")
    assert(dfOut.schema(1).name == "foo")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists") {
    val expectedOutput =
      """+-----+----------------------------------------------------------------------------+
        ||value|errCol                                                                      |
        |+-----+----------------------------------------------------------------------------+
        ||1    |[]                                                                          |
        ||1    |[]                                                                          |
        ||1    |[]                                                                          |
        ||1    |[[overWriteError, E000OW, Special column value has changed, value, [2], []]]|
        ||1    |[]                                                                          |
        |+-----+----------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = dfIn.withColumnIfDoesNotExist("value", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists, but has a different case") {
    val expectedOutput =
      """+-----+----------------------------------------------------------------------------+
        ||vAlUe|errCol                                                                      |
        |+-----+----------------------------------------------------------------------------+
        ||1    |[]                                                                          |
        ||1    |[]                                                                          |
        ||1    |[]                                                                          |
        ||1    |[[overWriteError, E000OW, Special column value has changed, vAlUe, [2], []]]|
        ||1    |[]                                                                          |
        |+-----+----------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = dfIn.withColumnIfDoesNotExist("vAlUe", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }
}
