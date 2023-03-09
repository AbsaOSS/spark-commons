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

package za.co.absa.spark.commons

import org.apache.spark.sql.SparkSession
import org.fusesource.hawtjni.runtime.Library

import java.util.concurrent.ConcurrentHashMap

/**
 * Abstract class to help attach/register UDFs and similar object only once to a spark session.
 * This is done by using the companion object's registry `ConcurrentHashMap` that holds already
 * instantiated classes thus not running the method [[register]] again on them.
 *
 * Usage: extend this abstract class and implement the method [[register]]. On initialization the
 * [[register]] method gets called by the [[za.co.absa.spark.commons.OncePerSparkSession$.registerMe]] method if the class + spark session
 * combination is unique. If it is not unique [[register]] will not get called again.
 * This way we ensure only single registration per spark session.
 *
 * @param sparkToRegisterTo Spark session to which we wish to attach objects
 */
abstract class OncePerSparkSession() extends Serializable {

  def this()(implicit sparkToRegisterTo: SparkSession) = {
    this()
    register(sparkToRegisterTo)
  }
  def register(implicit spark: SparkSession): Unit = {
    OncePerSparkSession.registerMe(this, spark)
  }

  protected def registerBody(spark: SparkSession): Unit

}

object OncePerSparkSession {
  private[this] type Key = (Int, String)

  private[this] val registry = new ConcurrentHashMap[Key, Unit]

  private[this] def makeKey(library: OncePerSparkSession, spark: SparkSession): Key = {
    (
      spark.hashCode(),  //using hash as sufficiently unique and allowing garbage collection
      library.getClass.getName
    )
  }

  protected def registerMe(library: OncePerSparkSession, spark: SparkSession): Unit = {
    // the function is `protected` to make it visible to `ScalaDoc`
    Option(registry.putIfAbsent(makeKey(library, spark), Unit))
      .getOrElse(library.registerBody(spark))
  }

}
