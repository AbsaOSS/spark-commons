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

package za.co.absa.spark.commons.schema

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.schema.SchemaUtils._

class SchemaUtilSuite extends AnyFunSuite with Matchers {
  // scalastyle:off magic.number


  test ("Test doesCastAlwaysSucceed()") {
    assert(!doesCastAlwaysSucceed(StructType(Seq()), StringType))
    assert(!doesCastAlwaysSucceed(ArrayType(StringType), StringType))
    assert(!doesCastAlwaysSucceed(StringType, ByteType))
    assert(!doesCastAlwaysSucceed(StringType, ShortType))
    assert(!doesCastAlwaysSucceed(StringType, IntegerType))
    assert(!doesCastAlwaysSucceed(StringType, LongType))
    assert(!doesCastAlwaysSucceed(StringType, DecimalType(10,10)))
    assert(!doesCastAlwaysSucceed(StringType, DateType))
    assert(!doesCastAlwaysSucceed(StringType, TimestampType))
    assert(!doesCastAlwaysSucceed(StructType(Seq()), StructType(Seq())))
    assert(!doesCastAlwaysSucceed(ArrayType(StringType), ArrayType(StringType)))

    assert(!doesCastAlwaysSucceed(ShortType, ByteType))
    assert(!doesCastAlwaysSucceed(IntegerType, ByteType))
    assert(!doesCastAlwaysSucceed(IntegerType, ShortType))
    assert(!doesCastAlwaysSucceed(LongType, ByteType))
    assert(!doesCastAlwaysSucceed(LongType, ShortType))
    assert(!doesCastAlwaysSucceed(LongType, IntegerType))

    assert(doesCastAlwaysSucceed(StringType, StringType))
    assert(doesCastAlwaysSucceed(ByteType, StringType))
    assert(doesCastAlwaysSucceed(ShortType, StringType))
    assert(doesCastAlwaysSucceed(IntegerType, StringType))
    assert(doesCastAlwaysSucceed(LongType, StringType))
    assert(doesCastAlwaysSucceed(DecimalType(10,10), StringType))
    assert(doesCastAlwaysSucceed(DateType, StringType))
    assert(doesCastAlwaysSucceed(TimestampType, StringType))
    assert(doesCastAlwaysSucceed(StringType, StringType))

    assert(doesCastAlwaysSucceed(ByteType, ByteType))
    assert(doesCastAlwaysSucceed(ByteType, ShortType))
    assert(doesCastAlwaysSucceed(ByteType, IntegerType))
    assert(doesCastAlwaysSucceed(ByteType, LongType))
    assert(doesCastAlwaysSucceed(ShortType, ShortType))
    assert(doesCastAlwaysSucceed(ShortType, IntegerType))
    assert(doesCastAlwaysSucceed(ShortType, LongType))
    assert(doesCastAlwaysSucceed(IntegerType, IntegerType))
    assert(doesCastAlwaysSucceed(IntegerType, LongType))
    assert(doesCastAlwaysSucceed(LongType, LongType))
    assert(doesCastAlwaysSucceed(DateType, TimestampType))
  }

  test("Test isCommonSubPath()") {
    assert (isCommonSubPath())
    assert (isCommonSubPath("a"))
    assert (isCommonSubPath("a.b.c.d.e.f", "a.b.c.d", "a.b.c", "a.b", "a"))
    assert (!isCommonSubPath("a.b.c.d.e.f", "a.b.c.x", "a.b.c", "a.b", "a"))
  }
}
