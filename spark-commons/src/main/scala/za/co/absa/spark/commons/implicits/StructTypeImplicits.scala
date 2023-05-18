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

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import za.co.absa.spark.commons.adapters.TransformAdapter
import za.co.absa.spark.commons.implicits.DataTypeImplicits.DataTypeEnhancements
import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldEnhancements
import za.co.absa.spark.commons.utils.SchemaUtils
import za.co.absa.spark.commons.utils.SchemaUtils.{getAllArraySubPaths, isCommonSubPath}

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Try

object StructTypeImplicits {

  implicit class DataFrameSelector(schema: StructType) extends TransformAdapter {
    //TODO  Fix ScalaDoc cross-module links #48 - DataFrameImplicits.DataFrameEnhancements.alignSchema
    /**
     * Returns data selector that can be used to align utils of a data frame.
     * You can use DataFrameImplicits.DataFrameEnhancements.alignSchema.
     *
     * @return Sorted DF to conform to utils
     */
    def getDataFrameSelector(): List[Column] = {

      def processArray(arrType: ArrayType, column: Column, name: String): Column = {
        arrType.elementType match {
          case arrType: ArrayType =>
            transform(column, x => processArray(arrType, x, name)).as(name)
          case nestedStructType: StructType =>
            transform(column, x => struct(processStruct(nestedStructType, Some(x)): _*)).as(name)
          case _ => column
        }
      }

      def processStruct(curSchema: StructType, parent: Option[Column]): List[Column] = {
        curSchema.foldRight(List.empty[Column])((field, acc) => {
          val currentCol: Column = parent match {
            case Some(x) => x.getField(field.name).as(field.name)
            case None => col(field.name)
          }
          field.dataType match {
            case arrType: ArrayType => processArray(arrType, currentCol, field.name) :: acc
            case structType: StructType => struct(processStruct(structType, Some(currentCol)): _*).as(field.name) :: acc
            case _ => currentCol :: acc
          }
        })
      }

      processStruct(schema, None)
    }

  }

  implicit class StructTypeEnhancements(val schema: StructType) {
    /**
     * Get a field from a text path and a given utils
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

      val pathTokens = SchemaUtils.splitPath(path)
      Try{
        examineStructField(pathTokens.tail, schema(pathTokens.head))
      }.getOrElse(None)
    }

    /**
     * Get a type of a field from a text path and a given utils
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
     * Get nullability of a field from a text path and a given utils
     *
     * @param path   The dot-separated path to the field
     * @return Some(nullable) or None if the field does not exist
     */
    def getFieldNullability(path: String): Option[Boolean] = {
      getField(path).map(_.nullable)
    }

    /**
     * Checks if a field specified by a path and a utils exists
     * @param path   The dot-separated path to the field
     * @return       True if the field exists false otherwise
     */
    def fieldExists(path: String): Boolean = {
      getField(path).nonEmpty
    }

    /**
     * Get paths for all array fields in the utils
     *
     * @return Seq of dot separated paths of fields in the utils, which are of type Array
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
     * @param path A column to check
     * @return true if the column is the only column in a struct
     */
    def isOnlyField(path: String): Boolean = {
      val pathSegments = SchemaUtils.splitPath(path)
      evaluateConditionsForField(schema, pathSegments, path, applyArrayHelper = false, applyLeafCondition = true,
        field => field.fields.length == 1)
    }

    /**
     * Compares 2 dataframe schemas.
     *
     * @param other The second utils to compare
     * @return true if provided schemas are the same ignoring nullability
     */
    def isEquivalent(other: StructType): Boolean = {
      val currentfields = schema.sortBy(_.name.toLowerCase)
      val fields2 = other.sortBy(_.name.toLowerCase)

      currentfields.size == fields2.size &&
        currentfields.zip(fields2).forall {
          case (f1, f2) =>
            f1.name.equalsIgnoreCase(f2.name) &&
              f1.dataType.isEquivalentDataType( f2.dataType)
        }
    }

    /**
     * Returns a list of differences in one utils to the other
     *
     * @param other The second utils to compare
     * @param parent  Parent path. Should be left default by the users first run. This is used for the accumulation of
     *                differences and their print out.
     * @return Returns a Seq of paths to differences in schemas
     */
    def diffSchema(other: StructType, parent: String = ""): Seq[String] = {
      val fields1 = getMapOfFields(schema)
      val fields2 = getMapOfFields(other)

      val diff = fields1.values.foldLeft(Seq.empty[String])((difference, field1) => {
        val field1NameLc = field1.name.toLowerCase()
        if (fields2.contains(field1NameLc)) {
          val field2 = fields2(field1NameLc)
          difference ++ field1.diffField(field2, parent)
        } else {
          difference ++ Seq(s"$parent.${field1.name} cannot be found in both schemas")
        }
      })

      diff.map(_.stripPrefix("."))
    }

    /**
     * Checks if the originalSchema is a subset of subsetSchema.
     *
     * @param originalSchema The utils that needs to have at least all t
     * @return true if provided schemas are the same ignoring nullability
     */
    def isSubset(originalSchema: StructType): Boolean = {
      val subsetFields = getMapOfFields(schema)
      val originalFields = getMapOfFields(originalSchema)

      subsetFields.forall(subsetField =>
        originalFields.contains(subsetField._1) &&
          subsetField._2.dataType.isEquivalentDataType(originalFields(subsetField._1).dataType))
    }

    private def getMapOfFields(schema: StructType): Map[String, StructField] = {
      schema.map(field => field.name.toLowerCase() -> field).toMap
    }

    def isOfType[T <: DataType](path: String)(implicit ev: ClassTag[T]): Boolean = {
      getFieldType(path) match {
        case Some(_: T) => true
        case _ => false
      }
    }

    protected def evaluateConditionsForField(structField: StructType, path: Seq[String], fieldPathName: String,
                                           applyArrayHelper: Boolean, applyLeafCondition: Boolean = false,
                                           conditionLeafSh: StructType => Boolean = _ => false): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0

      @tailrec
      def arrayHelper(fieldPathName: String, arrayField: ArrayType, path: Seq[String]): Boolean = {
        arrayField.elementType match {
          case st: StructType =>
            evaluateConditionsForField(st, path.tail, fieldPathName, applyArrayHelper, applyLeafCondition, conditionLeafSh)
          case ar: ArrayType => arrayHelper(fieldPathName, ar, path)
          case _ =>
            if (!isLeaf) {
              throw new IllegalArgumentException(
                s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
            }
            false
        }
      }

      structField.fields.exists(field =>
        if (field.name == currentField) {
          (field.dataType, isLeaf) match {
              case (st: StructType, false) =>
                evaluateConditionsForField(st, path.tail, fieldPathName, applyArrayHelper, applyLeafCondition, conditionLeafSh)
              case (_, true) if applyLeafCondition => conditionLeafSh(structField)
              case (_: ArrayType, true) => true
              case (ar: ArrayType, false) if applyArrayHelper => arrayHelper(fieldPathName, ar, path)
              case (_: ArrayType, false) => false
              case (_, false) => throw new IllegalArgumentException(s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
              case (_, _) => false
            }
        } else false
      )
    }
  }

  implicit class StructTypeEnhancementsArrays(schema: StructType) extends StructTypeEnhancements(schema) {
    /**
     * Get first array column's path out of complete path.
     *
     * E.g if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" will be returned.
     *
     * @param path The path to the attribute
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

      val pathToks = SchemaUtils.splitPath(path)
      helper(pathToks, Seq()).mkString(".")
    }

    /**
     * Get all array columns' paths out of complete path.
     *
     * E.g. if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" and "a.b.c.d" will be returned.
     *
     * @param path The path to the attribute
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

      val pathToks = SchemaUtils.splitPath(path)
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
     * @param paths A list of paths to analyze
     * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
     */
    def getDeepestCommonArrayPath(paths: Seq[String]): Option[String] = {
      val arrayPaths = paths.flatMap(path => getAllArraysInPath(path)).distinct

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
     * @param path A path to analyze
     * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
     */
    def getDeepestArrayPath(path: String): Option[String] = {
      val arrayPaths = getAllArraysInPath(path)

      if (arrayPaths.nonEmpty) {
        Some(arrayPaths.maxBy(_.length))
      } else {
        None
      }
    }

    /**
     * Checks if a field is an array that is not nested in another array
     *
     * @param path A field to check
     * @return true if a field is an array that is not nested in another array
     */
    def isNonNestedArray(path: String): Boolean = {
      val pathSegments = SchemaUtils.splitPath(path)
      evaluateConditionsForField(schema, pathSegments, path, applyArrayHelper = false)
    }
  }

}
