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

package za.co.absa.spark.commons.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import za.co.absa.spark.commons.utils.SchemaUtils

import scala.util.{Success, Try}

// scalastyle:off
object functions {
// scalastyle:on

  /**
   *  Similarly to `col` function evaluates the column based on the provided column name. But here, it can be a full
   *  path even of nested fields. It also evaluates arrays and maps where the array index or map key is in brackets `[]`.
   *
   * @param fullColName The full name of the column, where segments are separated by dots and array indexes or map keys
   *                    are in brackets
   * @return            The column per the specified path
   */
  def col_of_path(fullColName: String): Column = {
    def segmentToColumn(colFnc: String => Column, columnSegment: String): Column = {
      val PatternForSubfield = """^(.+)\[(.+)]$""".r
      columnSegment match {
        case PatternForSubfield(column, subfield) =>
          Try(subfield.toInt) match {
            case Success(arrayIndex) => colFnc(column)(arrayIndex)
            case _ => colFnc(column)(subfield)
          }
        case _ => colFnc(columnSegment)
      }
    }

    val segments = SchemaUtils.splitPath(fullColName)

    segments.tail.foldLeft(segmentToColumn(col, segments.head)) {case(acc, columnSegment) =>
      segmentToColumn(acc.apply, columnSegment)
    }
  }

}
