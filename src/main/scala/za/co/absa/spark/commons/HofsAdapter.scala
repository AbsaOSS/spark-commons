package za.co.absa.spark.commons

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Column
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.commons.version.Version
import za.co.absa.commons.version.Version.VersionStringInterpolator

import scala.reflect.runtime.universe._

trait HofsAdapter {

  /**
   * For Spark versions prior 3.0.0, delegates to {{{hofs.transform()}}}
   * Otherwise delegates to the native Spark method.
   */
  val transform: (Column, Column => Column) => Column = {
    val fnRefAST =
      if (Version.asSemVer(SPARK_VERSION) < semver"3.0.0")
        q"za.co.absa.spark.hofs.transform(_: Column, _: Column => Column)"
      else
        q"org.apache.spark.sql.functions.transform(_: Column, _: Column => Column)"
    ReflectionUtils.compile(
      q"""
          import org.apache.spark.sql.Column
          $fnRefAST
          """
    )(Map.empty)
  }

}
