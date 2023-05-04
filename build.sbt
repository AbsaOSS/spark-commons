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
lazy val spark2   = "2.4.7"
lazy val spark32   = "3.2.1"
lazy val spark33   = "3.3.1"

import Dependencies._
import SparkVersionAxis._

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

ThisBuild / versionScheme := Some("early-semver")

lazy val commonSettings = Seq(
  libraryDependencies ++= commonDependencies,
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
  Test / parallelExecution := false
)

lazy val commonJacocoExcludes: Seq[String] = Seq(
  "za.co.absa.spark.commons.adapters.CallUdfAdapter",
  "za.co.absa.spark.commons.adapters.TransformAdapter"
  //    "za.co.absa.spark.commons.utils.JsonUtils*", // class and related objects
  //    "za.co.absa.spark.commons.utils.ExplodeTools" // class only
)

lazy val parent = (project in file("."))
  .aggregate(sparkCommons.projectRefs ++ sparkCommonsTest.projectRefs: _*)
  .settings(
    name := "spark-commons-parent",
    publish / skip := true
  )

lazy val `sparkCommons` = (projectMatrix in file("spark-commons"))
  .settings(commonSettings: _*)
  .sparkRow(SparkVersionAxis(spark2), scalaVersions = Seq(scala211, scala212))
  .sparkRow(SparkVersionAxis(spark32), scalaVersions = Seq(scala212))
  .sparkRow(SparkVersionAxis(spark33), scalaVersions = Seq(scala212))
  .dependsOn(sparkCommonsTest % "test")

lazy val sparkCommonsTest = (projectMatrix in file("spark-commons-test"))
  .settings(
    commonSettings ++ Seq(
    name := "spark-commons-test",
    libraryDependencies ++= sparkDependencies(spark2)
    ): _*
  )
  .jvmPlatform(scalaVersions = Seq(scala211, scala212))
