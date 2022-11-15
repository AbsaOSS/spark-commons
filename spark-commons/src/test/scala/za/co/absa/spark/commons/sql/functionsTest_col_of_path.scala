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

package za.co.absa.spark.commons.sql

import org.apache.spark.sql.functions.col
import functions.col_of_path
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class functionsTest_col_of_path extends AnyFunSuite with SparkTestBase {

  import spark.implicits._

  test("col_of_path - simple column") {
    val columnName = "simple"
    val result = col_of_path(columnName)
    val expected = col(columnName)
    assert(result == expected)
  }

  test("col_of_path - including array and map columns") {
    val columnName = "array_column[42].in_between.another_array[0].map_column[map_key]"
    val result = col_of_path(columnName)
    val expected = col("array_column")(42)("in_between")("another_array")(0)("map_column")("map_key")
    assert(result == expected)
  }

  test("col_of_path - evaluate from data with map") {
    val sourceDF = Seq(
      (1, Map("a"->"x", "b"->"y", "c"->"z")),
      (2, Map("a"->"ooo", "b"->"ppp"))
    ).toDF("id", "map")
    val resultDF = sourceDF.withColumn("new", col_of_path("map[b]"))

    val expected = Seq(
      (1, Map("a"->"x", "b"->"y", "c"->"z"), "y"),
      (2, Map("a"->"ooo", "b"->"ppp"), "ppp")
    )

    val result = resultDF.as[(Int, Map[String, String], String)].collect().sortBy(_._1).toList
    assert(result == expected)
  }

  test("col_of_path - evaluate from data with array") {


    val a1 = Array(Node("One"), Node("Two", Option(SubNode("TwentyOne", 68))), Node("Three"))
    val a2 = Array(Node("Alpha"), Node("Beta", Option(SubNode("BetaOne", 42))))
    val sourceDF = Seq(
      (1, a1),
      (2, a2)
    ).toDF("id", "ar")

    val resultDF = sourceDF
      .withColumn("new1", col_of_path("ar[0].name"))
      .withColumn("new2", col_of_path("ar[1].subNode.value"))
      .withColumn("new3", col_of_path("ar[2]"))
      .select("id", "new1", "new2", "new3")

    resultDF.printSchema()
    resultDF.show(false)

    val expected = Seq(
      (1, "One", 68, Option(Node("Three"))),
      (2, "Alpha", 42, None)
    )

    val result = resultDF.as[(Int, String, Int, Option[Node])].collect().sortBy(_._1).toList
    assert(result == expected)
  }

}

private case class SubNode(name: String, value: Int)
private case class Node(name: String, subNode: Option[SubNode] = None)

