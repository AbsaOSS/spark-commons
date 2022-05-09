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

import org.apache.spark.sql.AnalysisException
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

class DataFrameImplicitsTest_WithHofsAdapter extends AnyFunSuite with SparkTestBase with JsonTestData {
  import spark.implicits._

  import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
  import za.co.absa.spark.commons.adapters.HofsTransformAdapter._

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
}
