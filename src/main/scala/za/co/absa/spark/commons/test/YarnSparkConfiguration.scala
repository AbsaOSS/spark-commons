package za.co.absa.spark.commons.test

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.iterableAsScalaIterable

class YarnSparkConfiguration(confDir: String, distJarsDir: String) extends SparkTestConfig{

  override def sparkSession = SparkSession.builder().config(new SparkConf().setAll(getHadoopConfigurationForSpark(confDir)))
    .config("spark.yarn.jars", getDependencies())
    .config("spark.deploy.mode", "client")
    .getOrCreate()

  def getDependencies(): String = {
    //get a list of all dist jars
    val distJars = FileSystem.get(getHadoopConfiguration(confDir))
      .listStatus(new Path(distJarsDir)).map(_.getPath)
    val localJars = getDepsFromClassPath("absa")
    val currentJars = getCurrentProjectJars
    (distJars ++ localJars ++currentJars).mkString(",")
  }

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
    hadoopConf.map(entry => (s"spark.hadoop.${entry.getKey}", entry.getValue)).toMap
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
