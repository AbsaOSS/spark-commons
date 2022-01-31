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

package za.co.absa.spark.commons.implicits

import org.apache.spark.sql.types._
import scala.util.Try

object StructFieldImplicits {
  implicit class StructFieldMetadataEnhancement(val metadata: Metadata) {
    def getOptString(key: String): Option[String] = {
      Try(metadata.getString(key)).toOption
    }

    def getOptChar(key: String): Option[Char] = {
      val resultString = Try(metadata.getString(key)).toOption
      resultString.flatMap { s =>
        if (s != null && s.length == 1) {
          Option(s(0))
        } else {
          None
        }
      }
    }

    def getOptStringAsBoolean(key: String): Option[Boolean] = {
      Try(metadata.getString(key).toBoolean).toOption
    }

    def hasKey(key: String): Boolean = {
      metadata.contains(key)
    }
  }
}
