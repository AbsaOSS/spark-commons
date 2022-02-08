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

trait JsonTestData {

  protected val jsonA = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}], "key" : {"alfa": "1", "beta": {"beta2": "2"}} }]"""
  protected val jsonB = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100,"price":10}]}]}]"""
  protected val jsonC = """[{"legs":[{"legid":100,"conditions":[{"amount":100,"checks":[{"checkNums":["1","2","3b","4","5c","6"]}]}]}],"id":1, "key" : {"beta": {"beta2": "2"}, "alfa": "1"} }]"""
  protected val jsonD = """[{"legs":[{"legid":100,"conditions":[{"amount":100,"checks":[{"checkNums":["1","2","3b","4","5c","6"]}]}]}],"id":1, "key" : {"beta": {"beta2": 2}, "alfa": 1} }]"""
  protected val jsonE = """[{"legs":[{"legid":100,"conditions":[{"amount":100,"checks":[{"checkNums":["1","2","3b","4","5c","6"]}]}]}],"id":1, "key" : {"beta": {"beta2": 2}, "alfa": 1}, "extra" : "a"}]"""

  protected val sample =
    """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}""" ::
      """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"amount":200}]}]}""" ::
      """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"amount": 300}]}]}""" ::
      """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"amount": 400}]}]}""" ::
      """{"id":5,"legs":[{"legid":500,"conditions":[]}]}""" ::
      """{"id":6,"legs":[]}""" ::
      """{"id":7}""" :: Nil

  protected val schema = StructType(Seq(
    StructField("a", IntegerType, nullable = false),
    StructField("b", StructType(Seq(
      StructField("c", IntegerType),
      StructField("d", StructType(Seq(
        StructField("e", IntegerType))), nullable = true)))),
    StructField("f", StructType(Seq(
      StructField("g", ArrayType.apply(StructType(Seq(
        StructField("h", IntegerType))))))))))

  protected val nestedSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", ArrayType(StructType(Seq(
      StructField("c", StructType(Seq(
        StructField("d", ArrayType(StructType(Seq(
          StructField("e", IntegerType))))))))))))))

  protected val arrayOfArraysSchema = StructType(Seq(
    StructField("a", ArrayType(ArrayType(IntegerType)), nullable = false),
    StructField("b", ArrayType(ArrayType(StructType(Seq(
      StructField("c", StringType, nullable = false)
    ))
    )), nullable = true)
  ))

}
