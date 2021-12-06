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

ThisBuild / name := "spark-commons"
ThisBuild / organization := "za.co.absa"
ThisBuild / scalaVersion := "2.12.12"

libraryDependencies ++=  List(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "org.scalatest"    %% "scalatest" % "3.0.5" % Test
)

mainClass in assembly := Some("za.co.absa.SparkApp")

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", _@_*) => MergeStrategy.last
  case PathList("javax", "inject", _@_*) => MergeStrategy.last
  case PathList("javax", "servlet", _@_*) => MergeStrategy.last
  case PathList("javax", "activation", _@_*) => MergeStrategy.last
  case PathList("org", "apache", _@_*) => MergeStrategy.last
  case PathList("com", "google", _@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", _@_*) => MergeStrategy.last
  case PathList("com", "codahale", _@_*) => MergeStrategy.last
  case PathList("com", "yammer", _@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
