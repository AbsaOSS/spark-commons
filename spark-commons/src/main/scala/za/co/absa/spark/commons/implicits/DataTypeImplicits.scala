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

object DataTypeImplicits {

  implicit class DataTypeEnhancements(val dt: DataType) extends AnyVal {
    /**
     * Wraps an array of StructField with StructType
     *
     * @param fieldSchema is the actual schema to be wrapped
     * @return StructType containing the StructFields
     */
    def wrapStructFiledWithStructType(fieldSchema: Option[DataType], errorColumnName: String): Boolean = {
      val st = StructType(Seq(StructField(errorColumnName, fieldSchema.head)))
      isEquivalentDataType(st)
    }
    /**
     * Compares 2 fields of a dataframe utils.
     *
     * @param other The second field to compare
     * @return true if provided fields are the same ignoring nullability
     */
    def isEquivalentDataType(other: DataType): Boolean = {
      dt match {
        case arrayType1: ArrayType =>
          other match {
            case arrayType2: ArrayType => arrayType1.isEquivalentArrayType(arrayType2)
            case _ => false
          }
        case structType1: StructType =>
          other match {
            case structType2: StructType => structType1.isEquivalent(structType2)
            case _ => false
          }
        case _ => dt == other
      }
    }

    /**
     * Checks if a casting between types always succeeds
     *
     * @param targetType A type to be casted to
     * @return true if casting never fails
     */
    def doesCastAlwaysSucceed(targetType: DataType): Boolean = {
      (dt, targetType) match {
        case (_: StructType, _) | (_: ArrayType, _) => false
        case (a, b) if a == b => true
        case (_, _: StringType) => true
        case (_: ByteType, _: ShortType | _: IntegerType | _: LongType) => true
        case (_: ShortType, _: IntegerType | _: LongType) => true
        case (_: IntegerType, _: LongType) => true
        case (_: DateType, _: TimestampType) => true
        case _ => false
      }
    }

    /**
     * Determine if a datatype is a primitive one
     */
    def isPrimitive(): Boolean = dt match {
      case _: BinaryType
           | _: BooleanType
           | _: ByteType
           | _: DateType
           | _: DecimalType
           | _: DoubleType
           | _: FloatType
           | _: IntegerType
           | _: LongType
           | _: NullType
           | _: ShortType
           | _: StringType
           | _: TimestampType => true
      case _ => false
    }
  }


}
