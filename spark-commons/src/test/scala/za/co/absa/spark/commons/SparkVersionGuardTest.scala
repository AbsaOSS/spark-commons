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

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.commons.version.Version.VersionStringInterpolator

class SparkVersionGuardTest extends AnyFunSuite {
  test("Guard with lower and upper bounds") {
    val guard = SparkVersionGuard(semver"2.0.0", Some(semver"3.0.0"))

    intercept[AssertionError] {
      guard.ensureSparkVersionCompatibility(semver"1.5.0")
    }
    guard.ensureSparkVersionCompatibility(semver"2.0.0")
    guard.ensureSparkVersionCompatibility(semver"3.0.0")
  }

  test("Guard with lower and without upper bounds") {
    val guard = SparkVersionGuard(semver"2.0.0", None)

    intercept[AssertionError] {
      guard.ensureSparkVersionCompatibility("1.5.0")
    }
    guard.ensureSparkVersionCompatibility("2.0.0")
    guard.ensureSparkVersionCompatibility("3.0.0")
  }
}
