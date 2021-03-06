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
import za.co.absa.spark.commons.implicits.ArrayTypeImplicits.ArrayTypeEnhancements
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

import scala.util.Try

object StructFieldImplicits {
  implicit class StructFieldMetadataEnhancements(val metadata: Metadata) extends AnyVal {
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

  implicit class StructFieldEnhancements(val structField: StructField) extends AnyVal {

    /**
     * Finds all differences of two StructFields and returns their paths
     *
     * @param other The second field to compare
     * @param parent Parent path. This is used for the accumulation of differences and their print out
     * @return Returns a Seq of found difference paths in scheme in the StructField
     */
    private[implicits] def diffField(other: StructField, parent: String): Seq[String] = {
      structField.dataType match {
        case _ if structField.dataType.typeName != other.dataType.typeName =>
          Seq(s"$parent.${structField.name} data type doesn't match (${structField.dataType.typeName}) vs (${other.dataType.typeName})")
        case arrayType1: ArrayType =>
          arrayType1.diffArray(other.dataType.asInstanceOf[ArrayType], s"$parent.${structField.name}")
        case structType1: StructType =>
          structType1.diffSchema(other.dataType.asInstanceOf[StructType], s"$parent.${structField.name}")
        case _ =>
          Seq.empty[String]
      }
    }
  }
}
