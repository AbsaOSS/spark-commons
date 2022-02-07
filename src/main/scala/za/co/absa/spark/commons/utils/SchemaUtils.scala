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

package za.co.absa.spark.commons.utils

import org.apache.spark.sql.types._
import za.co.absa.spark.commons.adapters.HofsAdapter


object SchemaUtils extends HofsAdapter {

  /**
   * Returns the parent path of a field. Returns an empty string if a root level field name is provided.
   *
   * @param columnName A fully qualified column name
   * @return The parent column name or an empty string if the input column is a root level column
   */
  def getParentPath(columnName: String): String = {
    val index = columnName.lastIndexOf('.')
    if (index > 0) {
      columnName.substring(0, index)
    } else {
      ""
    }
  }

  /**
   * Get paths for all array subfields of this given datatype
   * @param path   The path to the attribute
   * @param name   The name of the attribute
   * @param dt      Data type to be checked
   *
   */
  def getAllArraySubPaths(path: String, name: String, dt: DataType): Seq[String] = {
    val currPath = appendPath(path, name)
    dt match {
      case s: StructType => s.fields.flatMap(f => getAllArraySubPaths(currPath, f.name, f.dataType))
      case _@ArrayType(elType, _) => getAllArraySubPaths(path, name, elType) :+ currPath
      case _ => Seq()
    }
  }

  /**
   * For a given list of field paths determines if any path pair is a subset of one another.
   *
   * For instance,
   *  - 'a.b', 'a.b.c', 'a.b.c.d' have this property.
   *  - 'a.b', 'a.b.c', 'a.x.y' does NOT have it, since 'a.b.c' and 'a.x.y' have diverging subpaths.
   *
   * @param paths A list of paths to be analyzed
   * @return true if for all pathe the above property holds
   */
  def isCommonSubPath(paths: String*): Boolean = {
    def sliceRoot(paths: Seq[Seq[String]]): Seq[Seq[String]] = {
      paths.map(path => path.drop(1)).filter(_.nonEmpty)
    }

    var isParentCommon = true // For Seq() the property holds by [my] convention
    var restOfPaths: Seq[Seq[String]] = paths.map(_.split('.').toSeq).filter(_.nonEmpty)
    while (isParentCommon && restOfPaths.nonEmpty) {
      val parent = restOfPaths.head.head
      isParentCommon = restOfPaths.forall(path => path.head == parent)
      restOfPaths = sliceRoot(restOfPaths)
    }
    isParentCommon
  }

  /**
   * Append a new attribute to path or empty string.
   *
   * @param path      The dot-separated existing path
   * @param fieldName Name of the field to be appended to the path
   * @return The path with the new field appended or the field itself if path is empty
   */
  def appendPath(path: String, fieldName: String): String = {
    if (path.isEmpty) {
      fieldName
    } else if (fieldName.isEmpty) {
      path
    } else {
      s"$path.$fieldName"
    }
  }

}
