package za.co.absa.spark.commons

import org.apache.spark.sql.SparkSession

trait SparkTestBase {
  implicit val spark: SparkSession = SparkSession.builder()
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
