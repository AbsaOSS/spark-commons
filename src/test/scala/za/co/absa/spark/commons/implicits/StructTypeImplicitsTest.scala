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

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
import za.co.absa.spark.commons.test.SparkTestBase

class StructTypeImplicitsTest extends AnyFunSuite with SparkTestBase {
  // scalastyle:off magic.number

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

  private val arrayOfArraysSchema = StructType(Seq(
    StructField("a", ArrayType(ArrayType(IntegerType)), nullable = false),
    StructField("b", ArrayType(ArrayType(StructType(Seq(
      StructField("c", StringType, nullable = false)
    ))
    )), nullable = true)
  ))

  private val structFieldNoMetadata = StructField("a", IntegerType)

  private val structFieldWithMetadataNotSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
  private val structFieldWithMetadataSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_a").build)

  test("Testing getFieldType") {
    import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

    val a = schema.getFieldType("a")
    val b = schema.getFieldType("b")
    val c = schema.getFieldType("b.c")
    val d = schema.getFieldType("b.d")
    val e = schema.getFieldType("b.d.e")
    val f = schema.getFieldType("f")
    val g = schema.getFieldType("f.g")
    val h = schema.getFieldType("f.g.h")

    assert(a.get.isInstanceOf[IntegerType])
    assert(b.get.isInstanceOf[StructType])
    assert(c.get.isInstanceOf[IntegerType])
    assert(d.get.isInstanceOf[StructType])
    assert(e.get.isInstanceOf[IntegerType])
    assert(f.get.isInstanceOf[StructType])
    assert(g.get.isInstanceOf[ArrayType])
    assert(h.get.isInstanceOf[IntegerType])
    assert(schema.getFieldType("z").isEmpty)
    assert(schema.getFieldType("x.y.z").isEmpty)
    assert(schema.getFieldType("f.g.h.a").isEmpty)
  }

  test("Testing fieldExists") {
    assert(schema.fieldExists("a"))
    assert(schema.fieldExists("b"))
    assert(schema.fieldExists("b.c"))
    assert(schema.fieldExists("b.d"))
    assert(schema.fieldExists("b.d.e"))
    assert(schema.fieldExists("f"))
    assert(schema.fieldExists("f.g"))
    assert(schema.fieldExists("f.g.h"))
    assert(!schema.fieldExists("z"))
    assert(!schema.fieldExists("x.y.z"))
    assert(!schema.fieldExists("f.g.h.a"))
  }

  test("Test isColumnArrayOfStruct") {
    assert(!schema.isColumnArrayOfStruct("a"))
    assert(!schema.isColumnArrayOfStruct("b"))
    assert(!schema.isColumnArrayOfStruct("b.c"))
    assert(!schema.isColumnArrayOfStruct("b.d"))
    assert(!schema.isColumnArrayOfStruct("b.d.e"))
    assert(!schema.isColumnArrayOfStruct("f"))
    assert(schema.isColumnArrayOfStruct("f.g"))
    assert(!schema.isColumnArrayOfStruct("f.g.h"))
    assert(!nestedSchema.isColumnArrayOfStruct("a"))
    assert(nestedSchema.isColumnArrayOfStruct("b"))
    assert(nestedSchema.isColumnArrayOfStruct("b.c.d"))
  }


  test("Testing getAllArrayPaths") {
    assertResult(Seq("f.g"))(schema.getAllArrayPaths())
    val newSchema = schema("b").dataType.asInstanceOf[StructType]
    assertResult(Seq())(newSchema.getAllArrayPaths())
  }

  test("Testing getFieldNullability") {
    assert(schema.getFieldNullability("b.d").get)
    assert(schema.getFieldNullability("x.y.z").isEmpty)
  }

  test("Test isOnlyField()") {
    val schema = StructType(Seq[StructField](
      StructField("a", StringType),
      StructField("b", StructType(Seq[StructField](
        StructField("e", StringType),
        StructField("f", StringType)
      ))),
      StructField("c", StructType(Seq[StructField](
        StructField("d", StringType)
      )))
    ))

    assert(!schema.isOnlyField("a"))
    assert(!schema.isOnlyField("b.e"))
    assert(!schema.isOnlyField("b.f"))
    assert(schema.isOnlyField("c.d"))
  }

  test("Test getStructField on array of arrays") {
    assert(arrayOfArraysSchema.getField("a").contains(StructField("a", ArrayType(ArrayType(IntegerType)), nullable = false)))
    assert(arrayOfArraysSchema.getField("b").contains(StructField("b", ArrayType(ArrayType(StructType(Seq(StructField("c", StringType, nullable = false))))), nullable = true)))
    assert(arrayOfArraysSchema.getField("b.c").contains(StructField("c", StringType, nullable = false)))
    assert(arrayOfArraysSchema.getField("b.d").isEmpty)
  }

  test("Test fieldExists") {
    assert(schema.fieldExists("a"))
    assert(schema.fieldExists("b"))
    assert(schema.fieldExists("b.c"))
    assert(schema.fieldExists("b.d"))
    assert(schema.fieldExists("b.d.e"))
    assert(schema.fieldExists("f"))
    assert(schema.fieldExists("f.g"))
    assert(schema.fieldExists("f.g.h"))
    assert(!schema.fieldExists("z"))
    assert(!schema.fieldExists("x.y.z"))
    assert(!schema.fieldExists("f.g.h.a"))

    assert(arrayOfArraysSchema.fieldExists("a"))
    assert(arrayOfArraysSchema.fieldExists("b"))
    assert(arrayOfArraysSchema.fieldExists("b.c"))
    assert(!arrayOfArraysSchema.fieldExists("b.d"))
  }

  test("Test getClosestUniqueName() is working properly") {
    val schema = StructType(Seq[StructField](
      StructField("value", StringType),
      StructField("value_1", StringType),
      StructField("value_2", StringType)
    ))

    // A column name that does not exist
    val name1 = schema.getClosestUniqueName("v")
    // A column that exists
    val name2 = schema.getClosestUniqueName("value")

    assert(name1 == "v")
    assert(name2 == "value_3")
  }

}
