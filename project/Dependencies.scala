/*
 * Copyright 2018 ABSA Group Limited
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

  def sparkVersion: String = sys.props.getOrElse("SPARK_VERSION", "2.4.7")

  def rootDependencies(scalaVersion: String): Seq[ModuleID] = List(
    "org.apache.spark"   %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark"   %% "spark-sql" % sparkVersion % "provided",
    "za.co.absa.commons" %% "commons" % "1.0.0",
    "za.co.absa"         %% "spark-hofs" % "0.4.0",
    "org.scala-lang"     % "scala-compiler" % scalaVersion,
    "org.scalatest"      %% "scalatest" % "3.1.0" % Test,
    "org.scalatest"      %% "scalatest-flatspec" % "3.2.0" % Test,
    "org.scalatestplus"  %% "mockito-1-10" % "3.1.0.0" % Test
  )

  def sparkCommonsTestDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"   %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark"   %% "spark-sql" % sparkVersion % "provided"
  )
}
