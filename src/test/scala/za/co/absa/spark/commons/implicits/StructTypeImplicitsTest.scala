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

import org.apache.spark.sql.types.{ArrayType, ByteType, DateType, DecimalType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

class StructTypeImplicitsTest extends AnyFunSuite {
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

  test ("Test isColumnArrayOfStruct") {
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

  test("getRenamesInSchema - no renames") {
    val schema = StructType(Seq(
      structFieldNoMetadata,
      structFieldWithMetadataNotSourceColumn))
    val result = schema.getRenamesInSchema()
    assert(result.isEmpty)
  }

  test("getRenamesInSchema - simple rename") {
    val schema = StructType(Seq(structFieldWithMetadataSourceColumn))
    val result = schema.getRenamesInSchema()
    assert(result == Map("a" -> "override_a"))

  }

  test("getRenamesInSchema - complex with includeIfPredecessorChanged set") {
    val sub = StructType(Seq(
      StructField("d", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "o").build),
      StructField("e", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "e").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("a", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "x").build),
      StructField("b", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "b").build),
      StructField("c", sub)
    ))

    val includeIfPredecessorChanged = true
    val result = schema.getRenamesInSchema(includeIfPredecessorChanged)
    val expected = Map(
      "a"   -> "x"  ,
      "a.d" -> "x.o",
      "a.e" -> "x.e",
      "a.f" -> "x.f",
      "b.d" -> "b.o",
      "c.d" -> "c.o"
    )

    assert(result == expected)
  }

  test("getRenamesInSchema - complex with includeIfPredecessorChanged not set") {
    val sub = StructType(Seq(
      StructField("d", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "o").build),
      StructField("e", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "e").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("a", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "x").build),
      StructField("b", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "b").build),
      StructField("c", sub)
    ))

    val includeIfPredecessorChanged = false
    val result = schema.getRenamesInSchema(includeIfPredecessorChanged)
    val expected = Map(
      "a"   -> "x",
      "a.d" -> "x.o",
      "b.d" -> "b.o",
      "c.d" -> "c.o"
    )

    assert(result == expected)
  }


  test("getRenamesInSchema - array") {
    val sub = StructType(Seq(
      StructField("renamed", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "rename source").build),
      StructField("same", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "same").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("array1", ArrayType(sub)),
      StructField("array2", ArrayType(ArrayType(ArrayType(sub)))),
      StructField("array3", ArrayType(IntegerType), nullable = false, new MetadataBuilder().putString("sourcecolumn", "array source").build)
    ))

    val includeIfPredecessorChanged = false
    val result = schema.getRenamesInSchema(includeIfPredecessorChanged)
    val expected = Map(
      "array1.renamed" -> "array1.rename source",
      "array2.renamed" -> "array2.rename source",
      "array3"   -> "array source"
    )

    assert(result == expected)
  }


  test("getRenamesInSchema - source column used multiple times") {
    val sub = StructType(Seq(
      StructField("x", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build),
      StructField("y", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build)
    ))
    val schema = StructType(Seq(
      StructField("a", sub),
      StructField("b", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build)
    ))

    val result = schema.getRenamesInSchema()
    val expected = Map(
      "a.x" -> "a.src",
      "a.y" -> "a.src",
      "b"   -> "src"
    )

    assert(result == expected)
  }

  test("Testing getFirstArrayPath") {
    assertResult("f.g")(schema.getFirstArrayPath("f.g.h"))
    assertResult("f.g")(schema.getFirstArrayPath("f.g"))
    assertResult("")(schema.getFirstArrayPath("z.x.y"))
    assertResult("")(schema.getFirstArrayPath("b.c.d.e"))
  }

  test("Testing getAllArrayPaths") {
    assertResult(Seq("f.g"))(schema.getAllArrayPaths())
    val newSchema = schema("b").dataType.asInstanceOf[StructType]
    assertResult(Seq())(newSchema.getAllArrayPaths())
  }

  test("Testing getAllArraysInPath") {
    assertResult(Seq("b", "b.c.d"))(nestedSchema.getAllArraysInPath("b.c.d.e"))
  }

  test("Testing getFieldNullability") {
    assert(schema.getFieldNullability("b.d").get)
    assert(schema.getFieldNullability("x.y.z").isEmpty)
  }

  test("Test getDeepestCommonArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert (schema.getDeepestCommonArrayPath(Seq("a", "a.b")).isEmpty)
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StringType)))
      ))))

    val deepestPath = schema.getDeepestCommonArrayPath(Seq("a", "a.b"))

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a")
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
        StructField("b", ArrayType(StringType))))
      )))

    val deepestPath = schema.getDeepestCommonArrayPath(Seq("a", "a.b"))

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b")
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

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b.c")
  }

  test("Test getDeepestArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert (schema.getDeepestArrayPath("a.b").isEmpty)
  }

  test("Test getDeepestArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StringType)))
      ))))

    val deepestPath = schema.getDeepestArrayPath("a.b")

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a")
  }

  test("Test getDeepestArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
        StructField("b", ArrayType(StringType))))
      )))

    val deepestPath = schema.getDeepestArrayPath("a.b")
    val deepestPath2 = schema.getDeepestArrayPath("a")

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b")
    assert (deepestPath2.isEmpty)
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

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b.c")
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
    assert(!schema.isOnlyField( "b.f"))
    assert(schema.isOnlyField("c.d"))
  }

  test("Test getStructField on array of arrays") {
    assert(arrayOfArraysSchema.getField("a").contains(StructField("a",ArrayType(ArrayType(IntegerType)),nullable = false)))
    assert(arrayOfArraysSchema.getField("b").contains(StructField("b",ArrayType(ArrayType(StructType(Seq(StructField("c",StringType,nullable = false))))), nullable = true)))
    assert(arrayOfArraysSchema.getField("b.c").contains(StructField("c",StringType,nullable = false)))
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

}
