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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import za.co.absa.spark.commons.adapters.TransformAdapter
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

object StructTypeImplicitsExtra {

  implicit class DataFrameSelector(schema: StructType) extends StructTypeEnhancements(schema) with TransformAdapter {
    /**
     * Returns data selector that can be used to align utils of a data frame. You can use [[alignSchema]].
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

}
