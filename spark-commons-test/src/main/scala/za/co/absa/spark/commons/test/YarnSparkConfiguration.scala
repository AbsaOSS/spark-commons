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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import za.co.absa.spark.commons.test.YarnSparkConfiguration._

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class YarnSparkConfiguration(confDir: String, distJarsDir: String) extends SparkTestConfig {

  override def master: String = "yarn"

  override def appName: String = super.appName + " - Yarn"

  override protected def builder: SparkSession.Builder = {
    super.builder
      .config(new SparkConf().setAll(getHadoopConfigurationForSpark(confDir)))
      .config("spark.yarn.jars", dependencies)
      .config("spark.deploy.mode", "client")
  }

  protected def dependencies: String = {
    //get a list of all dist jars
    val distJars = FileSystem
      .get(getHadoopConfiguration(confDir))
      .listStatus(new Path(distJarsDir))
      .map(_.getPath)
    val localJars = getDepsFromClassPath("absa")
    val currentJars = getCurrentProjectJars
    (distJars ++ localJars ++currentJars).mkString(",")
  }

}

object YarnSparkConfiguration {

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
    hadoopConf.asScala.map(entry => (s"spark.hadoop.${entry.getKey}", entry.getValue)).toMap
  }

  /**
   * Get Hadoop configuration consumable by SparkConf
   */
  def getHadoopConfigurationForSpark(hadoopConfDir: String): Map[String, String] = {
    hadoopConfToSparkMap(getHadoopConfiguration(hadoopConfDir))
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
