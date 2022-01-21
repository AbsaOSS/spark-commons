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

import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import za.co.absa.spark.commons.schema.MetadataKeys
import za.co.absa.spark.commons.schema.SchemaUtils.{appendPath, getAllArraySubPaths, isCommonSubPath}

import scala.annotation.tailrec
import scala.util.Try

object StructTypeImplicits {
  implicit class StructTypeEnhancements(val schema: StructType) {
    /**
     * Get a field from a text path and a given schema
     * @param path   The dot-separated path to the field
     * @return       Some(the requested field) or None if the field does not exist
     */
    def getField(path: String): Option[StructField] = {

      @tailrec
      def goThroughArrayDataType(dataType: DataType): DataType = {
        dataType match {
          case ArrayType(dt, _) => goThroughArrayDataType(dt)
          case result => result
        }
      }

      @tailrec
      def examineStructField(names: List[String], structField: StructField): Option[StructField] = {
        if (names.isEmpty) {
          Option(structField)
        } else {
          structField.dataType match {
            case struct: StructType         => examineStructField(names.tail, struct(names.head))
            case ArrayType(el: DataType, _) =>
              goThroughArrayDataType(el) match {
                case struct: StructType => examineStructField(names.tail, struct(names.head))
                case _                  => None
              }
            case _                          => None
          }
        }
      }

      val pathTokens = path.split('.').toList
      Try{
        examineStructField(pathTokens.tail, schema(pathTokens.head))
      }.getOrElse(None)
    }

    /**
     * Get a type of a field from a text path and a given schema
     *
     * @param path   The dot-separated path to the field
     * @return Some(the type of the field) or None if the field does not exist
     */
    def getFieldType(path: String): Option[DataType] = {
      getField(path).map(_.dataType)
    }

    /**
     * Checks if the specified path is an array of structs
     *
     * @param path   The dot-separated path to the field
     * @return true if the field is an array of structs
     */
    def isColumnArrayOfStruct(path: String): Boolean = {
      getFieldType(path) match {
        case Some(dt) =>
          dt match {
            case arrayType: ArrayType =>
              arrayType.elementType match {
                case _: StructType => true
                case _ => false
              }
            case _ => false
          }
        case None => false
      }
    }

    /**
     * Get nullability of a field from a text path and a given schema
     *
     * @param path   The dot-separated path to the field
     * @return Some(nullable) or None if the field does not exist
     */
    def getFieldNullability(path: String): Option[Boolean] = {
      getField(path).map(_.nullable)
    }

    /**
     * Checks if a field specified by a path and a schema exists
     * @param path   The dot-separated path to the field
     * @return       True if the field exists false otherwise
     */
    def fieldExists(path: String): Boolean = {
      getField(path).nonEmpty
    }

    /**
     * Returns all renames in the provided schema.
     * @param includeIfPredecessorChanged  if set to true, fields are included even if their name have not changed but
     *                                     a predecessor's (parent, grandparent etc.) has
     * @return        the keys of the returned map are the columns' names after renames, the values are the source columns;
     *                the name are full paths denoted with dot notation
     */
    def getRenamesInSchema(includeIfPredecessorChanged: Boolean = true): Map[String, String] = {

      def getRenamesRecursively(path: String,
                                sourcePath: String,
                                struct: StructType,
                                renamesAcc: Map[String, String],
                                predecessorChanged: Boolean): Map[String, String] = {
        import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldEnhancements

        struct.fields.foldLeft(renamesAcc) { (renamesSoFar, field) =>
          val fieldFullName = appendPath(path, field.name)
          val fieldSourceName = field.getMetadataString(MetadataKeys.SourceColumn).getOrElse(field.name)
          val fieldFullSourceName = appendPath(sourcePath, fieldSourceName)

          val (renames, renameOnPath) = if ((fieldSourceName != field.name) || (predecessorChanged && includeIfPredecessorChanged)) {
            (renamesSoFar + (fieldFullName -> fieldFullSourceName), true)
          } else {
            (renamesSoFar, predecessorChanged)
          }

          field.dataType match {
            case st: StructType => getRenamesRecursively(fieldFullName, fieldFullSourceName, st, renames, renameOnPath)
            case at: ArrayType  => getStructInArray(at.elementType).fold(renames) { item =>
              getRenamesRecursively(fieldFullName, fieldFullSourceName, item, renames, renameOnPath)
            }
            case _              => renames
          }
        }
      }

      @tailrec
      def getStructInArray(dataType: DataType): Option[StructType] = {
        dataType match {
          case st: StructType => Option(st)
          case at: ArrayType => getStructInArray(at.elementType)
          case _ => None
        }
      }

      getRenamesRecursively("", "", schema, Map.empty, predecessorChanged = false)
    }

    /**
     * Get first array column's path out of complete path.
     *
     *  E.g if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" will be returned.
     *
     * @param path   The path to the attribute
     * @return The path of the first array field or "" if none were found
     */
    def getFirstArrayPath(path: String): String = {
      @tailrec
      def helper(remPath: Seq[String], pathAcc: Seq[String]): Seq[String] = {
        if (remPath.isEmpty) Seq() else {
          val currPath = (pathAcc :+ remPath.head).mkString(".")
          val currType = getFieldType(currPath)
          currType match {
            case Some(_: ArrayType) => pathAcc :+ remPath.head
            case Some(_) => helper(remPath.tail, pathAcc :+ remPath.head)
            case None => Seq()
          }
        }
      }

      val pathToks = path.split('.')
      helper(pathToks, Seq()).mkString(".")
    }

    /**
     * Get all array columns' paths out of complete path.
     *
     *  E.g. if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" and "a.b.c.d" will be returned.
     *
     * @param path   The path to the attribute
     * @return Seq of dot-separated paths for all array fields in the provided path
     */
    def getAllArraysInPath(path: String): Seq[String] = {
      @tailrec
      def helper(remPath: Seq[String], pathAcc: Seq[String], arrayAcc: Seq[String]): Seq[String] = {
        if (remPath.isEmpty) arrayAcc else {
          val currPath = (pathAcc :+ remPath.head).mkString(".")
          val currType = getFieldType(currPath)
          currType match {
            case Some(_: ArrayType) =>
              val strings = pathAcc :+ remPath.head
              helper(remPath.tail, strings, arrayAcc :+ strings.mkString("."))
            case Some(_) => helper(remPath.tail, pathAcc :+ remPath.head, arrayAcc)
            case None => arrayAcc
          }
        }
      }

      val pathToks = path.split("\\.")
      helper(pathToks, Seq(), Seq())
    }

    /**
     * For a given list of field paths determines the deepest common array path.
     *
     * For instance, if given 'a.b', 'a.b.c', 'a.b.c.d' where b and c are arrays the common deepest array
     * path is 'a.b.c'.
     *
     * If any of the arrays are on diverging paths this function returns None.
     *
     * The purpose of the function is to determine the order of explosions to be made before the dataframe can be
     * joined on a field inside an array.
     *
     * @param fieldPaths A list of paths to analyze
     * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
     */
    def getDeepestCommonArrayPath(fieldPaths: Seq[String]): Option[String] = {
      val arrayPaths = fieldPaths.flatMap(path => getAllArraysInPath(path)).distinct

      if (arrayPaths.nonEmpty && isCommonSubPath(arrayPaths: _*)) {
        Some(arrayPaths.maxBy(_.length))
      } else {
        None
      }
    }

    /**
     * For a field path determines the deepest array path.
     *
     * For instance, if given 'a.b.c.d' where b and c are arrays the deepest array is 'a.b.c'.
     *
     * @param fieldPath A path to analyze
     * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
     */
    def getDeepestArrayPath(fieldPath: String): Option[String] = {
      val arrayPaths = getAllArraysInPath(fieldPath)

      if (arrayPaths.nonEmpty) {
        Some(arrayPaths.maxBy(_.length))
      } else {
        None
      }
    }

    /**
     * Get paths for all array fields in the schema
     *
     * @return Seq of dot separated paths of fields in the schema, which are of type Array
     */
    def getAllArrayPaths(): Seq[String] = {
      schema.fields.flatMap(f => getAllArraySubPaths("", f.name, f.dataType)).toSeq
    }

    /**
     * Get a closest unique column name
     *
     * @param desiredName A prefix to use for the column name
     * @return A name that can be used as a unique column name
     */
    def getClosestUniqueName(desiredName: String): String = {
      def fieldExists(name: String): Boolean = schema.fields.exists(_.name.compareToIgnoreCase(name) == 0)

      if (fieldExists(desiredName)) {
        Iterator.from(1)
          .map(index => s"${desiredName}_${index}")
          .dropWhile(fieldExists).next()
      } else {
        desiredName
      }
    }

    /**
     * Checks if a field is the only field in a struct
     *
     * @param column A column to check
     * @return true if the column is the only column in a struct
     */
    def isOnlyField(column: String): Boolean = {
      val path = column.split('.')
      structArrayHelper(schema, path, column)(applyArrayHelper = false, field => field.fields.length == 1)
    }

    /**
     * Checks if a field is an array that is not nested in another array
     *
     * @param fieldPathName A field to check
     * @return true if a field is an array that is not nested in another array
     */
    def isNonNestedArray(fieldPathName: String): Boolean = {
      val path = fieldPathName.split('.')
      structArrayHelper(schema, path, fieldPathName)(applyArrayHelper = false)
    }

    /**
     * Checks if a field is an array
     *
     * @param fieldPathName A field to check
     * @return true if the specified field is an array
     */
    def isArray(fieldPathName: String): Boolean = {
      val path = fieldPathName.split('.')
      structArrayHelper(schema, path, fieldPathName)(applyArrayHelper = true)
    }

    @tailrec
    private def arrayHelper(fieldPathName: String, arrayField: ArrayType, path: Seq[String])
                           (applyArrayHelper: Boolean, conditionLeafSh: StructType => Boolean): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0

      arrayField.elementType match {
        case st: StructType =>
          structArrayHelper(st, path.tail, fieldPathName)(applyArrayHelper, conditionLeafSh)
        case ar: ArrayType => arrayHelper(fieldPathName, ar, path) (applyArrayHelper, conditionLeafSh)
        case _ =>
          if (!isLeaf) {
            throw new IllegalArgumentException(
              s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
          }
          false
      }
    }

    private def structArrayHelper(structField: StructType, path: Seq[String], fieldPathName: String)
                                 (applyArrayHelper: Boolean,
                                  conditionLeafSh: StructType => Boolean = _ => false): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0

      structField.fields.exists(field =>
        if (field.name == currentField) {
          if (isLeaf && conditionLeafSh(structField)) {
            true
          } else {
            field.dataType match {
              case st: StructType =>
                if (!isLeaf) {
                  structArrayHelper(st, path.tail, fieldPathName)(applyArrayHelper, conditionLeafSh)
                } else false
              case ar: ArrayType =>
                if (isLeaf) {
                  true
                } else if (applyArrayHelper) {
                  arrayHelper(fieldPathName, ar, path)(applyArrayHelper, conditionLeafSh)
                } else {
                  false
                }
              case _ =>
                if (!isLeaf) {
                  throw new IllegalArgumentException(
                    s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
                }
                false
            }
          }
        } else false
      )
    }
  }

}
