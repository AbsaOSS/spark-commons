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

package za.co.absa.spark.commons.test

import org.apache.spark.sql.SparkSession

object DefaultSparkConfiguration extends SparkTestConfig {
  override protected def appName: String = super.appName + " - local"

  override protected def master: String = "local[*]"

  override protected def builder: SparkSession.Builder = {
    super.builder
      .config("spark.ui.enabled", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.hive.convertMetastoreParquet", value = false)
      .config("fs.defaultFS", "file:/")
  }
}