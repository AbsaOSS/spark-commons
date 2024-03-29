/*
 * Copyright 2021 ABSA Group Limited
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

import sbt._

object Dependencies {

  def majorVersion(fullVersion: String): String = {
    fullVersion.split("\\.", 2).headOption.getOrElse(fullVersion)
  }

  def commonDependencies: Seq[ModuleID] = Seq(
    "org.scalatest"         %% "scalatest"     % "3.2.2"      % Test,
    "org.mockito"           %% "mockito-scala" % "1.17.12"    % Test
  )

  def sparkDependencies(sparkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"      %% "spark-core"  % sparkVersion % "provided",
    "org.apache.spark"      %% "spark-sql"   % sparkVersion % "provided"
  )

  def sparkCommonsDependencies(sparkVersion: String): Seq[ModuleID] = {
    Seq(
      "za.co.absa.commons"  %% "commons"     % "1.0.0",
      "za.co.absa"          %% "spark-hofs"  % "0.5.0",
      "za.co.absa"          %% "spark-hats"  % "0.3.0"
    ) ++
      sparkDependencies(sparkVersion)
  }
}
