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

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.commons.version.Version
import za.co.absa.commons.version.Version.VersionStringInterpolator
import za.co.absa.commons.version.impl.SemVer20Impl.SemanticVersion

object SparkVersionGuard {
  val minSpark2XVersionIncluded: SemanticVersion = semver"2.4.2"
  val maxSpark2XVersionExcluded: SemanticVersion = semver"3.0.0"

  val minSpark3XVersionIncluded: SemanticVersion = semver"3.0.0"
  val maxSpark3XVersionExcluded: SemanticVersion = semver"4.0.0"

  /**
   * Populates the version guard with the defaults
   */
  def fromDefaultSparkCompatibilitySettings: SparkVersionGuard =
    SparkVersionGuard(minSpark2XVersionIncluded, Some(maxSpark3XVersionExcluded))

  /**
   * Populates the version guard with the defaults for Spark 2.X
   */
  def fromSpark2XCompatibilitySettings: SparkVersionGuard =
    SparkVersionGuard(minSpark2XVersionIncluded, Some(maxSpark2XVersionExcluded))

  /**
   * Populates the version guard with the defaults for Spark 3.X
   */
  def fromSpark3XCompatibilitySettings: SparkVersionGuard =
    SparkVersionGuard(minSpark3XVersionIncluded, Some(maxSpark3XVersionExcluded))

}

/**
 * Setup Spark version guard with allowed min & max sem-ver version.
 *
 * @param minVersionInclusive lowest acceptable spark version (may be non-final)
 * @param maxVersionExclusive version supremum - first discouraged spark version (this and higher version usage issues a warning) if not None
 */
case class SparkVersionGuard(minVersionInclusive: SemanticVersion, maxVersionExclusive: Option[SemanticVersion])
                            (implicit log:Logger = LoggerFactory.getLogger(SparkVersionGuard.getClass)) {

  /**
   * String wrapper for [[SparkVersionGuard#ensureSparkVersionCompatibility(yourVersion:za\.co\.absa\.commons\.version\.impl\.SemVer20Impl\.SemanticVersion* ensureSparkVersionCompatibility(yourVersion: SemanticVersion): Unit]]
   *
   * @param yourVersion provided spark version
   */
  def ensureSparkVersionCompatibility(yourVersion: String): Unit =
    ensureSparkVersionCompatibility(Version.asSemVer(yourVersion))

  /**
   * Supplied version will be checked against the [[SparkVersionGuard]]'s. Note, `yourVersion` is
   * finalized when comparing to max in order to warn about non-final versions against a final guard (3.0.0-rc.1
   * would issue a warning when 3.0.0 is the max bound)
   *
   * @param yourVersion provided spark version
   */
  def ensureSparkVersionCompatibility(yourVersion: SemanticVersion): Unit = {
    assert(yourVersion >= minVersionInclusive,
      s"""Your Spark version is too low. This SparkJob can only run on Spark version ${minVersionInclusive.asString} or higher.
         |Your detected version was ${yourVersion.asString}""".stripMargin)

    maxVersionExclusive.foreach { max =>
      // `someVersion.core >= max` guards against e.g. 3.0.0-rc.1 being allowed when 3.0.0 should not be
      if(yourVersion.core >= max) {
        log.warn(s"""Your Spark version may be too high. This SparkJob is developed to run on Spark version core lower than ${max.asString}
           |Your detected version was ${yourVersion.asString}""".stripMargin)
      }
    }
  }

}


