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

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

import scala.annotation.tailrec

object ArrayTypeImplicits {

  implicit class ArrayTypeEnhancements(arrayType: ArrayType) {

    /**
     * Compares 2 array fields of a dataframe utils.
     *
     * @param other The second array to compare
     * @return true if provided arrays are the same ignoring nullability
     */
    @scala.annotation.tailrec
    final def isEquivalentArrayType(other: ArrayType): Boolean = {
      arrayType.elementType match {
        case arrayType1: ArrayType =>
          other.elementType match {
            case arrayType2: ArrayType => arrayType1.isEquivalentArrayType(arrayType2)
            case _ => false
          }
        case structType1: StructType =>
          other.elementType match {
            case structType2: StructType => structType1.isEquivalent(structType2)
            case _ => false
          }
        case _ => arrayType.elementType == other.elementType
      }
    }


    /**
     * Finds all differences of two ArrayTypes and returns their paths
     *
     * @param array2 The second array to compare
     * @param parent Parent path. This is used for the accumulation of differences and their print out
     * @return Returns a Seq of found difference paths in scheme in the Array
     */
    @scala.annotation.tailrec
    private[implicits] final def diffArray(array2: ArrayType, parent: String): Seq[String] = {
      arrayType.elementType match {
        case _ if arrayType.elementType.typeName != array2.elementType.typeName =>
          Seq(s"$parent data type doesn't match (${arrayType.elementType.typeName}) vs (${array2.elementType.typeName})")
        case arrayType1: ArrayType =>
          arrayType1.diffArray(array2.elementType.asInstanceOf[ArrayType], s"$parent")
        case structType1: StructType =>
          structType1.diffSchema(array2.elementType.asInstanceOf[StructType], s"$parent")
        case _ => Seq.empty[String]
      }
    }

    /**
     * For an array of arrays of arrays, ... get the final element type at the bottom of the array
     *
     * @return A non-array data type at the bottom of array nesting
     */
    final def getDeepestArrayType(): Unit = {
      @tailrec
      def getDeepestArrayTypeHelper(arrayType: ArrayType): DataType = {
        arrayType.elementType match {
          case a: ArrayType => getDeepestArrayTypeHelper(a)
          case b => b
        }
      }
      getDeepestArrayTypeHelper(arrayType)
    }
  }
}
