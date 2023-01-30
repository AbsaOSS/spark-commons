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

import org.apache.spark.sql.functions.{array, lit, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class DataFrameImplicitsTest extends AnyFunSuite with SparkTestBase with JsonTestData {
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

  private def isCached(df: DataFrame): Boolean = {
    val planToCache = df.queryExecution.analyzed
    df.sparkSession.sharedState.cacheManager.lookupCachedData(planToCache).isDefined
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
    val dfOut = dfIn.withColumnIfDoesNotExist((df: DataFrame, _) => df)("foo", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfOut.schema.length == 2)
    assert(dfOut.schema.head.name == "value")
    assert(dfOut.schema(1).name == "foo")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists") {
    val expectedOutput =
      """+-----+
        ||value|
        |+-----+
        ||1    |
        ||1    |
        ||1    |
        ||2    |
        ||1    |
        |+-----+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = dfIn.withColumnIfDoesNotExist((df: DataFrame, _) => df)("value", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists, but has a different case") {
    val expectedOutput =
      """+-----+------+
        ||value|errCol|
        |+-----+------+
        ||1    |[]    |
        ||1    |[]    |
        ||1    |[]    |
        ||2    |[]    |
        ||1    |[]    |
        |+-----+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val function: (DataFrame, String) => DataFrame = (df: DataFrame, _) => df.withColumn("errCol", lit(Array.emptyIntArray))
    val dfOut = dfIn.withColumnIfDoesNotExist(function)("vAlUe", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }

  test("order schemas for equal schemas") {
    val dfA = spark.read.json(Seq(jsonA).toDS)
    val dfC = spark.read.json(Seq(jsonC).toDS).select("legs", "id", "key")

    val dfA2Aligned = dfC.alignSchema(dfA.schema)

    assert(dfA.columns.toSeq == dfA2Aligned.columns.toSeq)
    assert(dfA.select("key").columns.toSeq == dfA2Aligned.select("key").columns.toSeq)
  }

  test("throw an error for DataFrames with different schemas") {
    val dfA = spark.read.json(Seq(jsonA).toDS)
    val dfB = spark.read.json(Seq(jsonB).toDS)

    assertThrows[AnalysisException]{
      dfA.alignSchema(dfB.schema)
    }
  }

  test("cast NullTypes to corresponding types by enforceTypeOnNullTypeFields") {
    val dfWithComplexTypes = spark.read.json(Seq(jsonF).toDS)
      .withColumn("nullShouldBeString", lit(null))
      .withColumn("nullShouldBeInteger", lit(null))
      .withColumn("nullShouldBeArrayOfIntegers", lit(null))
      .withColumn("nullShouldBeArrayOfArraysOfIntegers", lit(null))
      .withColumn("nullShouldBeArrayOfStructs", lit(null))
      .withColumn("nullShouldBeStruct", lit(null))
      .withColumn("shouldIgnoreNonNullTypeMismatch", lit("abc"))
      .withColumn("arrayOfNullShouldBeArrayOfIntegers", array(lit(null), lit(null)))
      .withColumn(
        "arrayOfArrayOfNullShouldBeArrayOfArrayOfStrings",
        array(array(lit(null), lit(null)))
      )
      .withColumn(
        "arrayOfStructs",
        array(
          struct(
            lit(null).as("nullShouldBeString"),
            lit(null).as("nullShouldBeInteger"),
            lit("abc").as("shouldIgnoreNonNullTypeMismatch"),
            array(lit(null), lit(null)).as("arrayOfNullShouldBeArrayOfIntegers")
          )
        )
      )
      .withColumn(
        "complexStruct",
        struct(
          lit(null).as("nullShouldBeString"),
          lit(null).as("nullShouldBeInteger"),
          lit("abc").as("shouldIgnoreNonNullTypeMismatch"),
          array(lit(1), lit(2), lit(3)).as("shouldIgnoreNonNullArrayTypeMismatch"),
          struct(
            lit(null).as("nullShouldBeString"),
            lit(null).as("nullShouldBeInteger"),
            lit("abc").as("shouldIgnoreNonNullTypeMismatch")
          ).as("nestedStruct"),
          array(lit(null), lit(null)).as("arrayOfNullShouldBeArrayOfIntegers")
        )
      )

    val targetSchema = StructType(
      Seq(
        StructField("id", LongType),
        // nullShouldBeInteger and nullShouldBeString are swapped comparing to dfWithComplexTypes
        // to ensure enforceTypeOnNullTypeFields converts by name
        StructField("nullShouldBeInteger", IntegerType),
        StructField("nullShouldBeString", StringType),
        StructField("nullShouldBeArrayOfIntegers", ArrayType(IntegerType)),
        StructField("nullShouldBeArrayOfArraysOfIntegers", ArrayType(ArrayType(IntegerType))),
        StructField(
          "nullShouldBeArrayOfStructs",
          ArrayType(
            StructType(
              Seq(StructField("a", StringType), StructField("b", DecimalType(28, 8)))
            )
          )
        ),
        StructField(
          "nullShouldBeStruct",
          StructType(
            Seq(StructField("a", StringType), StructField("b", DecimalType(28, 8)))
          )
        ),
        StructField("shouldIgnoreNonNullTypeMismatch", IntegerType, false),
        StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false),
        StructField("arrayOfArrayOfNullShouldBeArrayOfArrayOfStrings", ArrayType(ArrayType(StringType), false), false),
        StructField(
          "arrayOfStructs",
          ArrayType(
            StructType(
              Seq(
                // nullShouldBeInteger and nullShouldBeString are swapped comparing to dfWithComplexTypes
                // to ensure enforceTypeOnNullTypeFields converts by name
                StructField("nullShouldBeInteger", IntegerType),
                StructField("nullShouldBeString", StringType),
                StructField("shouldIgnoreNonNullTypeMismatch", IntegerType, false),
                StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false)
              )
            )
          ),
          false
        ),
        StructField(
          "complexStruct",
          StructType(
            Seq(
              // nullShouldBeInteger and nullShouldBeString are swapped comparing to dfWithComplexTypes
              // to ensure enforceTypeOnNullTypeFields converts by name
              StructField("nullShouldBeInteger", IntegerType),
              StructField("nullShouldBeString", StringType),
              StructField("shouldIgnoreNonNullTypeMismatch", IntegerType, false),
              StructField("shouldIgnoreNonNullArrayTypeMismatch", ArrayType(StringType, false), false),
              StructField(
                "nestedStruct",
                StructType(
                  Seq(
                    // nullShouldBeInteger and nullShouldBeString are swapped comparing to dfWithComplexTypes
                    // to ensure enforceTypeOnNullTypeFields converts by name
                    StructField("nullShouldBeInteger", IntegerType),
                    StructField("nullShouldBeString", StringType),
                    StructField("shouldIgnoreNonNullTypeMismatch", IntegerType, false)
                  )
                ),
                false
              ),
              StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false)
            )
          ),
          false
        )
      )
    )

    val actual = dfWithComplexTypes.enforceTypeOnNullTypeFields(targetSchema)

    assert(actual.count() === dfWithComplexTypes.count())

    val actualSchema = actual.schema
    val expectedSchema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("nullShouldBeString", StringType),
        StructField("nullShouldBeInteger", IntegerType),
        StructField("nullShouldBeArrayOfIntegers", ArrayType(IntegerType)),
        StructField("nullShouldBeArrayOfArraysOfIntegers", ArrayType(ArrayType(IntegerType))),
        StructField(
          "nullShouldBeArrayOfStructs",
          ArrayType(
            StructType(
              Seq(StructField("a", StringType), StructField("b", DecimalType(28, 8)))
            )
          )
        ),
        StructField(
          "nullShouldBeStruct",
          StructType(
            Seq(StructField("a", StringType), StructField("b", DecimalType(28, 8)))
          )
        ),
        StructField("shouldIgnoreNonNullTypeMismatch", StringType, false),
        StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false),
        StructField("arrayOfArrayOfNullShouldBeArrayOfArrayOfStrings", ArrayType(ArrayType(StringType), false), false),
        StructField(
          "arrayOfStructs",
          ArrayType(
            StructType(
              Seq(
                StructField("nullShouldBeString", StringType),
                StructField("nullShouldBeInteger", IntegerType),
                StructField("shouldIgnoreNonNullTypeMismatch", StringType, false),
                StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false)
              )
            ),
            false
          ),
          false
        ),
        StructField(
          "complexStruct",
          StructType(
            Seq(
              StructField("nullShouldBeString", StringType),
              StructField("nullShouldBeInteger", IntegerType),
              StructField("shouldIgnoreNonNullTypeMismatch", StringType, false),
              StructField("shouldIgnoreNonNullArrayTypeMismatch", ArrayType(IntegerType, false), false),
              StructField(
                "nestedStruct",
                StructType(
                  Seq(
                    StructField("nullShouldBeString", StringType),
                    StructField("nullShouldBeInteger", IntegerType),
                    StructField("shouldIgnoreNonNullTypeMismatch", StringType, false)
                  )
                ),
                false
              ),
              StructField("arrayOfNullShouldBeArrayOfIntegers", ArrayType(IntegerType), false)
            )
          ),
          false
        )
      )
    )

    assert(actualSchema === expectedSchema)
  }

  test("Check that cacheIfNotCachedYet caches the data") {
    //Verify  check test procedure
    val dfA = spark.read.json(Seq(jsonA).toDS)
    assert(!isCached(dfA))
    dfA.cache()
    assert(isCached(dfA))
    //Do the test
    val dfB = spark.read.json(Seq(jsonB).toDS)
    assert(!isCached(dfB))
    dfB.cacheIfNotCachedYet()
    assert(isCached(dfB))
  }
}
