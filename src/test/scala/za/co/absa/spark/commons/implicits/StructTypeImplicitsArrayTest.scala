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

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancementsArrays
import za.co.absa.spark.commons.test.SparkTestBase

class StructTypeImplicitsArrayTest extends AnyFunSuite with SparkTestBase {

  private val schema = StructType(Seq(
    StructField("a", IntegerType, nullable = false),
    StructField("b", StructType(Seq(
      StructField("c", IntegerType),
      StructField("d", StructType(Seq(
        StructField("e", IntegerType))), nullable = true)))),
    StructField("f", StructType(Seq(
      StructField("g", ArrayType.apply(StructType(Seq(
        StructField("h", IntegerType))))))))))

  private val nestedSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", ArrayType(StructType(Seq(
      StructField("c", StructType(Seq(
        StructField("d", ArrayType(StructType(Seq(
          StructField("e", IntegerType))))))))))))))

  test("Testing getFirstArrayPath") {
    assertResult("f.g")(schema.getFirstArrayPath("f.g.h"))
    assertResult("f.g")(schema.getFirstArrayPath("f.g"))
    assertResult("")(schema.getFirstArrayPath("z.x.y"))
    assertResult("")(schema.getFirstArrayPath("b.c.d.e"))
  }

  test("Testing getAllArraysInPath") {
    assertResult(Seq("b", "b.c.d"))(nestedSchema.getAllArraysInPath("b.c.d.e"))
  }

  val sample =
    """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}""" ::
      """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"amount":200}]}]}""" ::
      """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"amount": 300}]}]}""" ::
      """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"amount": 400}]}]}""" ::
      """{"id":5,"legs":[{"legid":500,"conditions":[]}]}""" ::
      """{"id":6,"legs":[]}""" ::
      """{"id":7}""" :: Nil
  import spark.implicits._
  val df = spark.read.json(sample.toDS)

  test("Test isNonNestedArray") {
    assert(df.schema.isNonNestedArray("legs"))
    assert(!df.schema.isNonNestedArray("legs.conditions"))
    assert(!df.schema.isNonNestedArray("legs.conditions.checks"))
    assert(!df.schema.isNonNestedArray("legs.conditions.checks.checkNums"))
    assert(!df.schema.isNonNestedArray("id"))
    assert(!df.schema.isNonNestedArray("legs.legid"))
  }

  test("Test isNonArray") {
    assert(df.schema.isOfType[ArrayType]("legs"))
    assert(df.schema.isOfType[ArrayType]("legs.conditions"))
    assert(df.schema.isOfType[ArrayType]("legs.conditions.checks"))
    assert(df.schema.isOfType[ArrayType]("legs.conditions.checks.checkNums"))
    assert(!df.schema.isOfType[ArrayType]("id"))
    assert(!df.schema.isOfType[ArrayType]("legs.legid"))
  }

  test("Test getDeepestCommonArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert(schema.getDeepestCommonArrayPath(Seq("a", "a.b")).isEmpty)
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StringType)))
      ))))

    val deepestPath = schema.getDeepestCommonArrayPath(Seq("a", "a.b"))

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a")
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
        StructField("b", ArrayType(StringType))))
      )))

    val deepestPath = schema.getDeepestCommonArrayPath(Seq("a", "a.b"))

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a.b")
  }

  test("Test getDeepestCommonArrayPath() for a path with several nested arrays of struct") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StructType(Seq[StructField](
          StructField("c", ArrayType(StructType(Seq[StructField](
            StructField("d", StructType(Seq[StructField](
              StructField("e", StringType))
            )))
          ))))
        )))
      )))))

    val deepestPath = schema.getDeepestCommonArrayPath(Seq("a", "a.b", "a.b.c.d.e", "a.b.c.d"))

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a.b.c")
  }

  test("Test getDeepestArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert(schema.getDeepestArrayPath("a.b").isEmpty)
  }

  test("Test getDeepestArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StringType)))
      ))))

    val deepestPath = schema.getDeepestArrayPath("a.b")

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a")
  }

  test("Test getDeepestArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
        StructField("b", ArrayType(StringType))))
      )))

    val deepestPath = schema.getDeepestArrayPath("a.b")
    val deepestPath2 = schema.getDeepestArrayPath("a")

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a.b")
    assert(deepestPath2.isEmpty)
  }

  test("Test getDeepestArrayPath() for a path with several nested arrays of struct") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StructType(Seq[StructField](
          StructField("c", ArrayType(StructType(Seq[StructField](
            StructField("d", StructType(Seq[StructField](
              StructField("e", StringType))
            )))
          ))))
        )))
      )))))

    val deepestPath = schema.getDeepestArrayPath("a.b.c.d.e")

    assert(deepestPath.nonEmpty)
    assert(deepestPath.get == "a.b.c")
  }

}
