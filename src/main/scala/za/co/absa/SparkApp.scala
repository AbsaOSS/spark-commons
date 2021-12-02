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

package za.co.absa

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("spark-commons Spark Job").getOrCreate()

    val rows = Seq(
      Row(1, "hello"),
      Row(2, "world")
    )

    val schema = StructType(List(
      StructField("number", IntegerType, nullable = true),
      StructField("word", StringType, nullable = true)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    println("count: ")
    println(df.count())

    spark.stop()
  }
}
