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

object SchemaUtils {

  /**
   * Extracts the parent path of a field. Returns an empty string if a root level column name is provided.
   *
   * @param columnName A fully qualified column name
   * @return The parent column name or an empty string if the input column is a root level column
   */
  def getParentPath(columnName: String): String = {
    //using a sub-function might be tiny bit less efficient, as it generates both part, but it ensures consistency
    columnPathAndCore(columnName)._1
  }

  /**
   * Extracts the field name of a fully qualified column name.
   *
   * @param columnName A fully qualified column name
   * @return The field name without the parent path or the whole string if the input column is a root level column
   */
  def stripParentPath(columnName: String): String = {
    //using a sub-function might be tiny bit less efficient, as it generates both part, but it ensures consistency
    columnPathAndCore(columnName)._2
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
      case s: StructType => s.fields.toSeq.flatMap(f => getAllArraySubPaths(currPath, f.name, f.dataType))
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
    var restOfPaths: Seq[Seq[String]] = paths.map(SchemaUtils.splitPath).filter(_.nonEmpty)
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


  /**
   * Separates the field name components of a fully qualified column name as their hierarchy goes from root down to the
   * deepest one. No validation on the field names is done
   * Example: `"com.my.package.xyz"` -> `List("com", "my", "package", "xyz")`
   * Trailing '.' is ignored, leading one not.
   *
   * @param columnName      A fully qualified column name
   * @return Each level field name in sequence how they go from root to the lowest one
   */
  def splitPath(columnName: String): List[String] = splitPath(columnName, keepEmptyFields = true)

  /**
   * Separates the field name components of a fully qualified column name as their hierarchy goes from root down to the
   * deepest one. No validation on the field names is done
   * Function is rather overloaded than using default parameter for easier use in functions like `map`
   * Example: `"com.my.package.xyz"` -> `List("com", "my", "package", "xyz")`
   * Trailing '.' is ignored, leading one not.
   *
   * @param columnName      A fully qualified column name
   * @param keepEmptyFields If `false` any empty field names are removed from the result list, otherwise kept
   * @return Each level field name in sequence how they go from root to the lowest one
   */
  def splitPath(columnName: String, keepEmptyFields: Boolean): List[String] = {
    val stripped = columnName.stripSuffix(".")

    if (stripped.isEmpty) {
      List.empty
    } else {
      val segments = stripped.split('.').toList
      if (keepEmptyFields) {
        segments
      } else {
        segments.filterNot(_.isEmpty)
      }
    }
  }

  private def columnPathAndCore(columnName: String): (String, String) = {
    val index = columnName.lastIndexOf('.')
    if (index >= 0) {
      (columnName.take(index), columnName.drop((index + 1)))
    } else {
      ("", columnName)
    }

  }
}
