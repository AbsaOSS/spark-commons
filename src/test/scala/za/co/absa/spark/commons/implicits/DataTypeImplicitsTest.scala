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

import org.apache.spark.sql.types.{ArrayType, ByteType, DateType, DecimalType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.DataTypeImplicits.DataTypeEnhancements

class DataTypeImplicitsTest extends AnyFunSuite with JsonTestData {

  test ("Test doesCastAlwaysSucceed()") {
    assert(!StructType(Seq()).doesCastAlwaysSucceed(StringType))
    assert(!ArrayType(StringType).doesCastAlwaysSucceed(StringType))
    assert(!StringType.doesCastAlwaysSucceed(ByteType))
    assert(!StringType.doesCastAlwaysSucceed(ShortType))
    assert(!StringType.doesCastAlwaysSucceed(IntegerType))
    assert(!StringType.doesCastAlwaysSucceed(LongType))
    assert(!StringType.doesCastAlwaysSucceed(DecimalType(10,10)))
    assert(!StringType.doesCastAlwaysSucceed(DateType))
    assert(!StringType.doesCastAlwaysSucceed(TimestampType))
    assert(!StructType(Seq()).doesCastAlwaysSucceed(StructType(Seq())))
    assert(!ArrayType(StringType).doesCastAlwaysSucceed(ArrayType(StringType)))

    assert(!ShortType.doesCastAlwaysSucceed(ByteType))
    assert(!IntegerType.doesCastAlwaysSucceed(ByteType))
    assert(!IntegerType.doesCastAlwaysSucceed(ShortType))
    assert(!LongType.doesCastAlwaysSucceed(ByteType))
    assert(!LongType.doesCastAlwaysSucceed(ShortType))
    assert(!LongType.doesCastAlwaysSucceed(IntegerType))

    assert(StringType.doesCastAlwaysSucceed(StringType))
    assert(ByteType.doesCastAlwaysSucceed(StringType))
    assert(ShortType.doesCastAlwaysSucceed(StringType))
    assert(IntegerType.doesCastAlwaysSucceed(StringType))
    assert(LongType.doesCastAlwaysSucceed(StringType))
    assert(DecimalType(10,10).doesCastAlwaysSucceed(StringType))
    assert(DateType.doesCastAlwaysSucceed(StringType))
    assert(TimestampType.doesCastAlwaysSucceed(StringType))
    assert(StringType.doesCastAlwaysSucceed(StringType))

    assert(ByteType.doesCastAlwaysSucceed(ByteType))
    assert(ByteType.doesCastAlwaysSucceed(ShortType))
    assert(ByteType.doesCastAlwaysSucceed(IntegerType))
    assert(ByteType.doesCastAlwaysSucceed(LongType))
    assert(ShortType.doesCastAlwaysSucceed(ShortType))
    assert(ShortType.doesCastAlwaysSucceed(IntegerType))
    assert(ShortType.doesCastAlwaysSucceed(LongType))
    assert(IntegerType.doesCastAlwaysSucceed(IntegerType))
    assert(IntegerType.doesCastAlwaysSucceed(LongType))
    assert(LongType.doesCastAlwaysSucceed(LongType))
    assert(DateType.doesCastAlwaysSucceed(TimestampType))
  }

}
