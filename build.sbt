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

ThisBuild / organization := "za.co.absa"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"

import Dependencies._

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

lazy val printSparkScalaVersion = taskKey[Unit]("Print Spark and Scala versions spark-commons is being built for.")
ThisBuild / printSparkScalaVersion := {
    val log = streams.value.log
    log.info(s"Building with Spark ${sparkVersion}, Scala ${scalaVersion.value}")
}

lazy val parent = (project in file("."))
  .aggregate(sparkCommons, sparkCommonsTest)
  .settings(
    name := "spark-commons-parent",
    libraryDependencies ++= sparkCommonsDependencies(),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    publish / skip := true
  )

lazy val sparkCommons = (project in file("spark-commons"))
  .settings(
    name := "spark-commons",
    libraryDependencies ++= sparkCommonslibraryDependencies(scalaVersion.value),
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value // printSparkScalaVersion is run with compile
  ).dependsOn(sparkCommonsTest)

lazy val sparkCommonsTest = (project in file("spark-commons-test"))
  .settings(
    name := "spark-commons-test",
    libraryDependencies ++= sparkCommonsDependencies(),
    Test / parallelExecution := false,
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value // printSparkScalaVersion is run with compile
  )

releasePublishArtifactsAction := PgpKeys.publishSigned.value
