package za.co.absa.spark.commons.test

import java.util.TimeZone
import org.apache.spark.sql.SparkSession

object DefaultSparkConfiguration extends SparkTestConfig {

  override def sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(s"Commons unit testing SchemaUtils")
    .config("spark.ui.enabled", "false")
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.hive.convertMetastoreParquet", false)
    .config("fs.defaultFS", "file:/")
    .getOrCreate()
  def timezone: TimeZone = TimeZone.getDefault
  def appName: String = s"Commons unit testing"
}
