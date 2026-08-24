/*
 * Copyright 2019 ABSA Group Limited
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

addSbtPlugin("com.github.sbt"     % "sbt-ci-release"    % "1.11.2")

addSbtPlugin("de.heikoseeberger"  % "sbt-header"        % "5.7.0")

addSbtPlugin("com.eed3si9n"       % "sbt-projectmatrix" % "0.9.0")

addSbtPlugin("com.jsuereth" %% "scala-arm" % "2.0" from "https://repo1.maven.org/maven2/com/jsuereth/scala-arm_2.11/2.0/scala-arm_2.11-2.0.jar")
addSbtPlugin("com.jsuereth" %% "scala-arm" % "2.0" from "https://repo1.maven.org/maven2/com/jsuereth/scala-arm_2.12/2.0/scala-arm_2.12-2.0.jar")

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

addSbtPlugin("io.github.moranaapps" % "jacoco-method-filter-sbt" % "2.2.0")
