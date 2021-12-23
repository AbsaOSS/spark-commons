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

import java.io.File

import za.co.absa.spark.commons.time.TimeZoneNormalizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

trait SparkTestBase {self =>
  TimeZoneNormalizer.normalizeJVMTimeZone()

  val config: Config = ConfigFactory.load()
  val sparkMaster: String = "Master"

  val sparkBuilder: SparkSession.Builder = SparkSession.builder()
    .master(sparkMaster)
    .appName(s"Enceladus test - ${self.getClass.getName}")
    .config("spark.ui.enabled", "false")
    .config("spark.debug.maxToStringFields", 100) // scalastyle:ignore magic.number
  // ^- default value is insufficient for some tests, 100 is a compromise between resource consumption and expected need

  implicit val spark: SparkSession = if (sparkMaster == "yarn") {
    val confDir = config.getString("enceladus.utils.testUtils.hadoop.conf.dir")
    val distJarsDir = config.getString("enceladus.utils.testUtils.spark.distJars.dir")
    val sparkHomeDir = config.getString("enceladus.utils.testUtils.spark.home.dir")

    val hadoopConfigs = SparkTestBase.getHadoopConfigurationForSpark(confDir)
    val sparkConfigs = SparkTestBase.loadSparkDefaults(sparkHomeDir)
    val allConfigs = hadoopConfigs ++ sparkConfigs

    //get a list of all dist jars
    val distJars = FileSystem.get(SparkTestBase.getHadoopConfiguration(confDir)).listStatus(new Path(distJarsDir)).map(_.getPath)
    val localJars = SparkTestBase.getDepsFromClassPath("absa")
    val currentJars = SparkTestBase.getCurrentProjectJars
    val deps = (distJars ++ localJars ++currentJars).mkString(",")

    sparkBuilder.config(new SparkConf().setAll(allConfigs))
      .config("spark.yarn.jars", deps)
      .config("spark.deploy.mode", "client")
      .getOrCreate()

  } else {
    sparkBuilder
      .master("local[*]")
      .appName(s"Commons unit testing SchemaUtils")
      .config("spark.ui.enabled", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.hive.convertMetastoreParquet", false)
      .config("fs.defaultFS", "file:/")
      .getOrCreate()
  }
  TimeZoneNormalizer.normalizeSessionTimeZone(spark)

  // Do not display INFO entries for tests
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

}

object SparkTestBase {
  /**
   * Gets a Hadoop configuration object from the specified hadoopConfDir parameter
   *
   * @param hadoopConfDir string representation of HADOOP_CONF_DIR
   */
  def getHadoopConfiguration(hadoopConfDir: String): Configuration = {
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))
    hadoopConf.addResource(new Path(s"$hadoopConfDir/yarn-site.xml"))
    hadoopConf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))

    hadoopConf
  }

  /**
   * Converts all entries from a Hadoop configuration to Map, which can be consumed by SparkConf
   *
   * @param hadoopConf Hadoop Configuration object to be converted into Spark configs
   */
  def hadoopConfToSparkMap(hadoopConf: Configuration): Map[String, String] = {
    hadoopConf.iterator().map(entry => (s"spark.hadoop.${entry.getKey}", entry.getValue)).toMap
  }

  /**
   * Get Hadoop configuration consumable by SparkConf
   */
  def getHadoopConfigurationForSpark(hadoopConfDir: String): Map[String, String] = {
    hadoopConfToSparkMap(getHadoopConfiguration(hadoopConfDir))
  }

  /**
   * Loads spark defaults from the specified SPARK_HOME directory
   */
  def loadSparkDefaults(sparkHome: String): Map[String, String] = {
    val sparkConfigIn = ConfigFactory.empty().atPath(s"$sparkHome/conf/spark-defaults.conf")
    sparkConfigIn
      .entrySet()
      .filter(_.getKey != "spark.yarn.jars")
      .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
      .toMap
  }

  /**
   * Gets the list of jars, which are currently loaded in the classpath and contain the given inclPattern in the file name
   */
  def getDepsFromClassPath(inclPattern: String): Seq[String] = {
    val cl = this.getClass.getClassLoader
    cl.asInstanceOf[java.net.URLClassLoader].getURLs.filter(c => c.toString.contains(inclPattern)).map(_.toString())
  }

  /**
   * Get the list of jar(s) of the current project
   */
  def getCurrentProjectJars: Seq[String] = {
    val targetDir = new File(s"${System.getProperty("user.dir")}/target")
    targetDir
      .listFiles()
      .filter(f => f.getName.split("\\.").last.toLowerCase() == "jar" && f.getName.contains("original"))
      .map(_.getAbsolutePath)
  }
}
