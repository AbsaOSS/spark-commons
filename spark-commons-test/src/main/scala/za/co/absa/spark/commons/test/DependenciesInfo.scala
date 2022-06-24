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

package za.co.absa.spark.commons.test

import org.apache.spark.sql.SparkSession

trait DependenciesInfo {
  protected def spark: SparkSession

  def sparkVersion: String = spark.version
  def scalaVersion: String = util.Properties.versionNumberString

  def dependenciesInfo: String = {
    val line = s"Running Spark $sparkVersion on Scala $scalaVersion"
    val spaceCount = 1.max(80 - 2 - 1 - line.length) //at least one space, otherwise original length - starting * and space ending * and text length
    val spaces = " " * spaceCount
    val result = "\n********************************************************************************\n" +
                s"* $line$spaces*\n" +
                 "********************************************************************************"
    result
  }

}
