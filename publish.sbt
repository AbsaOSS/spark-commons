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

ThisBuild / organizationHomepage := Some(url("https://www.absa.africa"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/AbsaOSS/spark-commons/tree/master"),
    connection = "scm:git:git://github.com/AbsaOSS/spark-commons.git",
    devConnection = "scm:git:ssh://github.com/AbsaOSS/spark-commons.git"
  )
)

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / developers := List(
  Developer(
    id    = "Zejnilovic",
    name  = "Sasa Zejnilovic",
    email = "sasa.zejnilovic@absa.africa",
    url   = url("https://github.com/Zejnilovic")
  ),
  Developer(
    id    = "benedeki",
    name  = "David Benedeki",
    email = "david.benedeki@absa.africa",
    url   = url("https://github.com/benedeki")
  ),
  Developer(
    id    = "dk1844",
    name  = "Daniel Kavan",
    email = "daniel.kavan@absa.africa",
    url   = url("https://github.com/dk1844")
  ),
  Developer(
    id    = "AdrianOlosutean",
    name  = "Adrian Olosutean",
    email = "adrian.olosutean@absa.africa",
    url   = url("https://github.com/AdrianOlosutean")
  ),
  Developer(
    id    = "lsulak",
    name  = "Ladislav Sulak",
    email = "ladislav.sulak@absa.africa",
    url   = url("https://github.com/lsulak")
  )
)

ThisBuild / homepage := Some(url("https://github.com/AbsaOSS/spark-commomns"))
ThisBuild / description := "Spark Common Utilities for ABSA"
