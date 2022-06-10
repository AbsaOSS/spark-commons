/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.commons

import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import za.co.absa.spark.commons.test.SparkTestBase

class SparkCommonsDependenciesReport extends AnyFunSuite with SparkTestBase {

  private val logger = LoggerFactory.getLogger(this.getClass)


  test("Log versions") {
    val line = s"Running Spark ${spark.version} on Scala ${util.Properties.versionNumberString}"
    val spaceCount = 1.max(80 - 2 - 1 - line.length) //at least one space, otherwise original length - starting * and space ending * and text length
    val spaces = " " * spaceCount

    logger.info("********************************************************************************")
    logger.info((s"* $line$spaces*"))
    logger.info("********************************************************************************")
  }
}
