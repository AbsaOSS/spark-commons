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

import sbt._
import sbt.Keys._
import sbt.VirtualAxis._
import sbt.internal.ProjectMatrix
import sbtprojectmatrix.ProjectMatrixKeys._
import Dependencies._

case class SparkVersionAxis(sparkVersion: String) extends sbt.VirtualAxis.WeakAxis {
  val sparkVersionMajor: String = majorVersion(sparkVersion)
  override val directorySuffix = s"-spark${sparkVersionMajor}"
  override val idSuffix: String = s"_spark${sparkVersionMajor}_"
}

object SparkVersionAxis {
  private def makeModuleName(origName: String, sparkAxis: SparkVersionAxis): String = {
    origName
      .replaceAll("([A-Z])", "-$1")
      .toLowerCase()
      .replaceAllLiterally("spark", s"spark${sparkAxis.sparkVersionMajor}")
  }

  implicit class ProjectExtension(val p: ProjectMatrix) extends AnyVal {

    def sparkRow(sparkAxis: SparkVersionAxis, scalaVersions: Seq[String], settings: Def.SettingsDefinition*): ProjectMatrix =
      p.customRow(
        scalaVersions = scalaVersions,
        axisValues = Seq(sparkAxis, VirtualAxis.jvm),
        _.settings(
            moduleName := makeModuleName(name.value, sparkAxis),
            libraryDependencies ++= sparkCommonsDependencies(sparkAxis.sparkVersion)
        ).settings(settings: _*)
      )
  }
}
