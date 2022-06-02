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

package za.co.absa.spark.commons.utils.explode

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancementsArrays
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.utils.{ExplodeTools, JsonUtils}
import za.co.absa.spark.hats.Extensions._

import scala.io.Source

class ExplosionSuite extends AnyFunSuite with SparkTestBase {

  private val logger = LoggerFactory.getLogger(this.getClass)

  import spark.implicits._


  test("Test explosion of a simple array") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val sourceDf = sampleArray.toDF()

    val expectedSchema = readSchemaFromResource("/data/simple_array_explosion/exploded_schema.json")
    val expectedResults = readDfFromResource("/data/simple_array_explosion/exploded_data.json", expectedSchema)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", sourceDf)

    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema, expectedSchema)
    assertResults(explodedDf, expectedResults)
  }

  test("Test a simple array reconstruction") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val sourceDf = sampleArray.toDF().withColumn("static", lit(1))

    val expectedExplodedSchema = readSchemaFromResource("/data/simple_array_reconstruction/exploded_schema.json")
    val expectedExplodedResults = readDfFromResource("/data/simple_array_reconstruction/exploded_data.json", expectedExplodedSchema)
    val expectedRestoredSchema = readSchemaFromResource("/data/simple_array_reconstruction/restored_schema.json")
    val expectedRestoredResults = readDfFromResource("/data/simple_array_reconstruction/restored_data.json", expectedRestoredSchema)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", sourceDf)

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if explosion has been done correctly
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema, expectedExplodedSchema)
    assertResults(explodedDf, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema, expectedRestoredSchema)
    assertResults(restoredDf, expectedRestoredResults)
  }

  test("Test a array of array sequence of explosions") {
    // Example provided by Saša Zejnilović
    val sampleMatrix = List(
      List(
        List(1, 2, 3, 4, 5, 6),
        List(7, 8, 9, 10, 11, 12, 13)
      ), List(
        List(201, 202, 203, 204, 205, 206),
        List(207, 208, 209, 210, 211, 212, 213)
      ), List(
        List(301, 302, 303, 304, 305, 306),
        List(307, 308, 309, 310, 311, 312, 313)
      ), List(
        List(401, 402, 403, 404, 405, 406),
        List(407, 408, 409, 410, 411, 412, 413)
      )
    )
    val sourceDf = sampleMatrix.toDF().withColumn("static", lit(1))

    val expectedExplodedSchema = readSchemaFromResource("/data/array_of_array_explosions/exploded_schema.json")
    val expectedExplodedResults = readDfFromResource("/data/array_of_array_explosions/exploded_data.json", expectedExplodedSchema)
    val expectedRestoredSchema = readSchemaFromResource("/data/array_of_array_explosions/restored_schema.json")
    val expectedRestoredResults = readDfFromResource("/data/array_of_array_explosions/restored_data.json", expectedRestoredSchema)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("value", sourceDf)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("value", explodedDf1, explodeContext1)

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf2, explodeContext2)

    // Checking if explosion has been done correctly
    assert(explodeContext2.explosions.size == 2)
    assertSchema(explodedDf2.schema, expectedExplodedSchema)
    assertResults(explodedDf2, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema, expectedRestoredSchema)
    assertResults(restoredDf, expectedRestoredResults)
  }

  test("Test handling of empty and null arrays") {
    val sample = Seq("""{"value":[1,2,3,4,5,6,7,8,9,10],"static":1}""",
      """{"value":[2,3,4,5,6,7,8,9,10,11],"static":2}""",
      """{"value":[],"static":3}""",
      """{"static":4}""")
    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val expectedExplodedSchema = readSchemaFromResource("/data/empty_and_null_arrays/exploded_schema.json")
    val expectedExplodedResults = readDfFromResource("/data/empty_and_null_arrays/exploded_data.json", expectedExplodedSchema)
    val expectedRestoredSchema = readSchemaFromResource("/data/empty_and_null_arrays/restored_schema.json")
    val expectedRestoredResults = readDfFromResource("/data/empty_and_null_arrays/restored_data.json", expectedRestoredSchema)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", sourceDf)

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if explosion has been done correctly
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema, expectedExplodedSchema)
    assertResults(explodedDf, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema, expectedRestoredSchema)
    assertResults(restoredDf, expectedRestoredResults)
  }

  test("Test deconstruct()") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val expectedDeconstructedSchema = readSchemaFromResource("/data/deconstruct/deconstructed_data_schema.json")
    val expectedDeconstructedData = readDfFromResource("/data/deconstruct/deconstructed_data.json", expectedDeconstructedSchema)

    val expectedRestoredSchema = readSchemaFromResource("/data/deconstruct/restored_data_schema.json")
    val expectedRestoredData = readDfFromResource("/data/deconstruct/restored_data.json", expectedRestoredSchema)

    val deconstruct = ExplodeTools.deconstructNestedColumn(sourceDf, "leg.conditions")
    val (deconstructedDf, deconstructedCol, transientCol) = ExplodeTools.DeconstructedNestedField.unapply(deconstruct).get

    val reconstructedDf = ExplodeTools.nestedRenameReplace(deconstructedDf, deconstructedCol, "leg.conditions", transientCol)

    assertSchema(deconstructedDf.schema, expectedDeconstructedSchema)
    assertResults(deconstructedDf, expectedDeconstructedData)

    assertSchema(reconstructedDf.schema, expectedRestoredSchema)
    assertResults(reconstructedDf, expectedRestoredData)
  }

  test("Test multiple nesting of arrays and structs") {
    val sample = """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}""" ::
      """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"amount":200}]}]}""" ::
      """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"amount": 300}]}]}""" ::
      """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"amount": 400}]}]}""" ::
      """{"id":5,"legs":[{"legid":500,"conditions":[]}]}""" ::
      """{"id":6,"legs":[]}""" ::
      """{"id":7}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val expectedOriginalSchema = readSchemaFromResource("/data/nesting_arrays_and_structs/original_schema.json")
    val expectedOriginalResults = readDfFromResource("/data/nesting_arrays_and_structs/original_data.json", expectedOriginalSchema)
    val expectedExplodedSchema = readSchemaFromResource("/data/nesting_arrays_and_structs/exploded_schema.json")
    val expectedExplodedResults = readDfFromResource("/data/nesting_arrays_and_structs/exploded_data.json", expectedExplodedSchema)
    val expectedRestoredSchema = readSchemaFromResource("/data/nesting_arrays_and_structs/restored_schema.json")
    val expectedRestoredResults = readDfFromResource("/data/nesting_arrays_and_structs/restored_data.json", expectedRestoredSchema)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("legs", sourceDf)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("legs.conditions", explodedDf1, explodeContext1)
    val (explodedDf3, explodeContext3) = ExplodeTools.explodeArray("legs.conditions.checks", explodedDf2, explodeContext2)
    val (explodedDf4, explodeContext4) = ExplodeTools.explodeArray("legs.conditions.checks.checkNums", explodedDf3, explodeContext3)

    val explodeConditionFilter = explodeContext4.getControlFrameworkFilter
    val expectedExplodeFilter = "((((true AND (coalesce(legs_conditions_checks_checkNums_idx, 0) = 0)) AND (coalesce(legs_conditions_checks_idx, 0) = 0)) AND (coalesce(legs_conditions_idx, 0) = 0)) AND (coalesce(legs_idx, 0) = 0))"

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf4, explodeContext4)

    assert(sourceDf.schema.isNonNestedArray("legs"))
    assert(!sourceDf.schema.isNonNestedArray("legs.conditions"))
    assert(!sourceDf.schema.isNonNestedArray("legs.conditions.checks"))
    assert(!sourceDf.schema.isNonNestedArray("legs.conditions.checks.checkNums"))
    assert(!sourceDf.schema.isNonNestedArray("id"))
    assert(!sourceDf.schema.isNonNestedArray("legs.legid"))

    assertSchema(sourceDf.schema, expectedOriginalSchema)
    assertResults(sourceDf, expectedOriginalResults)

    assertSchema(explodedDf4.schema, expectedExplodedSchema)
    assertResults(explodedDf4, expectedExplodedResults)

    assertSchema(restoredDf.schema, expectedRestoredSchema)
    assertResults(restoredDf, expectedRestoredResults)

    // Check the filter generator as well
    assert(explodeConditionFilter.toString == expectedExplodeFilter)
  }

  test("Test exploding a nested array that is the only element of a struct") {
    val sample = """{"id":1,"leg":{"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"conditions":[]}}""" ::
      """{"id":4}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val expectedOriginalSchema = readSchemaFromResource("/data/array_in_struct/original_schema.json")
    val expectedExplodedSchema = readSchemaFromResource("/data/array_in_struct/exploded_schema.json")
    val expectedRestoredSchema = readSchemaFromResource("/data/array_in_struct/restored_schema.json")

    val expectedOriginalResults = readDfFromResource("/data/array_in_struct/original_data.json", expectedOriginalSchema)
    val expectedExplodedResults = readDfFromResource("/data/array_in_struct/exploded_data.json", expectedExplodedSchema)
    val expectedRestoredResults = readDfFromResource("/data/array_in_struct/restored_data.json", expectedRestoredSchema)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", sourceDf)
    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    assertSchema(sourceDf.schema, expectedOriginalSchema)
    assertResults(sourceDf, expectedOriginalResults)

    assertSchema(explodedDf.schema, expectedExplodedSchema)
    assertResults(explodedDf, expectedExplodedResults)

    assertSchema(restoredDf.schema, expectedRestoredSchema)
    assertResults(restoredDf, expectedRestoredResults)
  }

  test("Test explosion of an array field inside a struct") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", sourceDf)
    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    val expectedSchema = readSchemaFromResource("/data/array_inside_struct/restored_schema.json")
    val expectedData = readDfFromResource("/data/array_inside_struct/restored_data.json", expectedSchema)

    assertSchema(restoredDf.schema, expectedSchema)
    assertResults(restoredDf, expectedData)
  }

  test("Test explosion with an error column") {
    val sample = """{"id":1,"errors":["Error 1","Error 2"],"leg":{"legid":100,"conditions":[{"check":"1","action":"b"},{"check":"2","action":"d"},{"check":"3","action":"f"}]}}""" ::
      """{"id":2,"errors":[],"leg":{"legid":200,"conditions":[{"check":"0","action":"b"}]}}""" ::
      """{"id":3,"errors":[],"leg":{"legid":300}}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", sourceDf)

    // Manipulate error column
    val changedDf = explodedDf.select(concat($"errors", array($"leg.conditions.check")).as("errors"),
      $"id", $"leg", $"leg_conditions_id", $"leg_conditions_size", $"leg_conditions_idx")

    val restoredDf = ExplodeTools.revertAllExplosions(changedDf, explodeContext, Some("errors"))

    val expectedSchema = readSchemaFromResource("/data/explosion_error_column/restored_schema.json")
    val expectedData = readDfFromResource("/data/explosion_error_column/restored_data.json", expectedSchema)

    assertSchema(restoredDf.schema, expectedSchema)
    assertResults(restoredDf, expectedData)
  }

  test("Test empty struct inside an array") {
    val sample = """{"order":1,"a":[{"b":"H1","c":[{"d":1,"toDrop": "drop me"}]}],"myFlag":true}""" ::
      """{"order":2,"a":[{"b":"H2","c":[]}],"myFlag":true}""" ::
      """{"order":3,"a":[{"b":"H3"}],"myFlag":true}""" ::
      """{"order":4,"a":[{}],"myFlag":true}""" ::
      """{"order":5,"a":[],"myFlag":true}""" ::
      """{"order":6,"myFlag":true}""" :: Nil

    val sourceDf = JsonUtils.getDataFrameFromJson(sample)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("a", sourceDf)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("a.c", explodedDf1, explodeContext1)

    // Manipulate the exploded structs
    val changedDf = explodedDf2.nestedDropColumn("a.c.toDrop")

    val restoredDf = ExplodeTools.revertAllExplosions(changedDf, explodeContext2)

    val expectedSchema = readSchemaFromResource("/data/empty_struct_in_array/restored_schema.json")
    val expectedData = readDfFromResource("/data/empty_struct_in_array/restored_data.json", expectedSchema)

    assertSchema(restoredDf.schema, expectedSchema)
    assertResults(restoredDf, expectedData)
  }

  test("Test empty struct inside an array with the only array field") {
    val sample = """{"order":1,"a":[{"c":[{"d":1}]}],"myFlag":true}""" ::
      """{"order":2,"a":[{"c":[]}],"myFlag":true}""" ::
      """{"order":3,"a":[{}],"myFlag":true}""" ::
      """{"order":4,"a":[],"myFlag":true}""" ::
      """{"order":5,"myFlag":true}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(sample)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("a", df)

    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("a.c", explodedDf1, explodeContext1)

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf2, explodeContext2)

    val expectedSchema = readSchemaFromResource("/data/empty_struct_in_array_only_field/restored_schema.json")
    val expectedData = readDfFromResource("/data/empty_struct_in_array_only_field/restored_data.json", expectedSchema)

    assertSchema(restoredDf.schema, expectedSchema)
    assertResults(restoredDf, expectedData)
  }

  private def assertSchema(actualSchema: StructType, expectedSchema: StructType): Unit = {
    assert(actualSchema.isEquivalent(expectedSchema))
  }

  private def assertResults(actualResults: DataFrame, expectedResults: DataFrame): Unit = {
    val ar = actualResults.collect()
    val er = expectedResults.collect()
    if (!ar.sameElements(er)) {
      actualResults.show(false)
      println("  differs from\n")
      expectedResults.show(false)
      fail()
    }
  }

  private def readSchemaFromResource(filename: String): StructType = {
    JsonUtils.getSchemaFromJson(readResourceFileAsListOfLines(filename))
  }

  private def readDfFromResource(filename: String, schema: StructType): DataFrame = {
    JsonUtils.getDataFrameFromJson(readResourceFileAsListOfLines(filename), schema)
  }

  private def readResourceFileAsListOfLines(filename: String): List[String] = {
    val url = getClass.getResource(filename)

    val sourceFile = Source.fromFile(url.getFile)
    try {
      sourceFile.getLines().toList // making it a List to copy the content of the file into memory before it's closed
    } finally {
      sourceFile.close()
    }
  }

}
