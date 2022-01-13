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

package za.co.absa.spark.commons

import org.apache.spark.sql.types.{ArrayType, ByteType, DateType, DecimalType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.schema.SchemaUtils._

class SchemaUtilSuite extends AnyFunSuite with Matchers {
  // scalastyle:off magic.number


  test ("Test isCastAlwaysSucceeds()") {
    assert(!isCastAlwaysSucceeds(StructType(Seq()), StringType))
    assert(!isCastAlwaysSucceeds(ArrayType(StringType), StringType))
    assert(!isCastAlwaysSucceeds(StringType, ByteType))
    assert(!isCastAlwaysSucceeds(StringType, ShortType))
    assert(!isCastAlwaysSucceeds(StringType, IntegerType))
    assert(!isCastAlwaysSucceeds(StringType, LongType))
    assert(!isCastAlwaysSucceeds(StringType, DecimalType(10,10)))
    assert(!isCastAlwaysSucceeds(StringType, DateType))
    assert(!isCastAlwaysSucceeds(StringType, TimestampType))
    assert(!isCastAlwaysSucceeds(StructType(Seq()), StructType(Seq())))
    assert(!isCastAlwaysSucceeds(ArrayType(StringType), ArrayType(StringType)))

    assert(!isCastAlwaysSucceeds(ShortType, ByteType))
    assert(!isCastAlwaysSucceeds(IntegerType, ByteType))
    assert(!isCastAlwaysSucceeds(IntegerType, ShortType))
    assert(!isCastAlwaysSucceeds(LongType, ByteType))
    assert(!isCastAlwaysSucceeds(LongType, ShortType))
    assert(!isCastAlwaysSucceeds(LongType, IntegerType))

    assert(isCastAlwaysSucceeds(StringType, StringType))
    assert(isCastAlwaysSucceeds(ByteType, StringType))
    assert(isCastAlwaysSucceeds(ShortType, StringType))
    assert(isCastAlwaysSucceeds(IntegerType, StringType))
    assert(isCastAlwaysSucceeds(LongType, StringType))
    assert(isCastAlwaysSucceeds(DecimalType(10,10), StringType))
    assert(isCastAlwaysSucceeds(DateType, StringType))
    assert(isCastAlwaysSucceeds(TimestampType, StringType))
    assert(isCastAlwaysSucceeds(StringType, StringType))

    assert(isCastAlwaysSucceeds(ByteType, ByteType))
    assert(isCastAlwaysSucceeds(ByteType, ShortType))
    assert(isCastAlwaysSucceeds(ByteType, IntegerType))
    assert(isCastAlwaysSucceeds(ByteType, LongType))
    assert(isCastAlwaysSucceeds(ShortType, ShortType))
    assert(isCastAlwaysSucceeds(ShortType, IntegerType))
    assert(isCastAlwaysSucceeds(ShortType, LongType))
    assert(isCastAlwaysSucceeds(IntegerType, IntegerType))
    assert(isCastAlwaysSucceeds(IntegerType, LongType))
    assert(isCastAlwaysSucceeds(LongType, LongType))
    assert(isCastAlwaysSucceeds(DateType, TimestampType))
  }

  test("Test isCommonSubPath()") {
    assert (isCommonSubPath())
    assert (isCommonSubPath("a"))
    assert (isCommonSubPath("a.b.c.d.e.f", "a.b.c.d", "a.b.c", "a.b", "a"))
    assert (!isCommonSubPath("a.b.c.d.e.f", "a.b.c.x", "a.b.c", "a.b", "a"))
  }

  test("unpath - empty string remains empty") {
    val result = unpath("")
    val expected = ""
    assert(result == expected)
  }

  test("unpath - underscores get doubled") {
    val result = unpath("one_two__three")
    val expected = "one__two____three"
    assert(result == expected)
  }

  test("unpath - dot notation conversion") {
    val result = unpath("grand_parent.parent.first_child")
    val expected = "grand__parent_parent_first__child"
    assert(result == expected)
  }
}
