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

lazy val scala212 = "2.12.21"
lazy val scala213 = "2.13.18"
lazy val spark34 =  "3.4.4"
lazy val spark35  = "3.5.5"
lazy val spark40  = "4.0.2"
lazy val spark41  = "4.1.2"

import Dependencies.*
import SparkVersionAxis.*

ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213)

ThisBuild / versionScheme := Some("early-semver")

lazy val commonSettings = Seq(
  libraryDependencies ++= commonDependencies,
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings"),
  javacOptions ++= Seq("-source", "17", "-target", "17", "-Xlint"),
  Test / parallelExecution := false,
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "-Xmx2048m",
    "--add-modules=jdk.incubator.vector",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dio.netty.allocator.type=pooled",
    "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE",
    "--enable-native-access=ALL-UNNAMED"
  )
)

lazy val jacocoReportSettings = Seq(
  jmfReportFile   := Some(target.value / "jmf-report.json"),
  jmfReportFormat := "json"
)

lazy val parent = (project in file("."))
  .aggregate(
    sparkCommons.projectRefs ++
      sparkCommonsTest.projectRefs: _*
  )
  .settings(
    name := "spark-commons-parent",
    publish / skip := true
  )
  .enablePlugins(JacocoFilterPlugin)

lazy val sparkCommons = (projectMatrix in file("spark-commons"))
  .settings(commonSettings: _*)
  .settings(jacocoReportSettings: _*)
  .sparkRow(SparkVersionAxis(spark34), scalaVersions = Seq(scala212))
  .sparkRow(SparkVersionAxis(spark35), scalaVersions = Seq(scala212))
  .sparkRow(SparkVersionAxis(spark40), scalaVersions = Seq(scala213))
  .sparkRow(SparkVersionAxis(spark41), scalaVersions = Seq(scala213))
  .dependsOn(sparkCommonsTest % "test")
  .enablePlugins(JacocoFilterPlugin)

lazy val sparkCommonsTest = (projectMatrix in file("spark-commons-test"))
  .settings(
    commonSettings ++ Seq(
      name := "spark-commons-test",
      libraryDependencies ++= sparkDependencies(
        if (scalaVersion.value == scala212) spark35 else spark41
      ),
      Compile / unmanagedSourceDirectories += {
        val sourceDir = (Compile / sourceDirectory).value
        if (scalaVersion.value.startsWith("2.13")) {
          sourceDir / "scala_2.13+"
        } else {
          sourceDir / "scala_2.13-"
        }
      }
    ): _*
  )
  .jvmPlatform(scalaVersions = Seq(scala212, scala213))
