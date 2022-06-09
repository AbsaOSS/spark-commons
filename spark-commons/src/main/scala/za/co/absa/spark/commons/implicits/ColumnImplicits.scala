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
import org.apache.spark.sql.functions._

object ColumnImplicits {
  implicit class ColumnEnhancements(val column: Column) extends AnyVal{
    def isInfinite: Column = {
      column.isin(Double.PositiveInfinity, Double.NegativeInfinity)
    }

    /**
     * Spark strings are based on 1 unlike scala. The function shifts the substring indexation to be in accordance with
     * Scala/ Java.
     * Another enhancement is, that the function allows a negative index, denoting counting of the index from back
     * This version takes the substring from the startPos until the end.
     *
     * @param startPos the index (zero based) where to start the substring from, if negative it's counted from end
     * @return column with requested substring
     */
    def zeroBasedSubstr(startPos: Int): Column = {
      if (startPos >= 0) {
        zeroBasedSubstr(startPos, Int.MaxValue - startPos)
      } else {
        zeroBasedSubstr(startPos, -startPos)
      }
    }

    /**
     * Spark strings are base on 1 unlike scala. The function shifts the substring indexation to be in accordance with
     * Scala/ Java.
     * Another enhancement is, that the function allows a negative index, denoting counting of the index from back
     * This version takes the substring from the startPos and takes up to the given number of characters (less.
     *
     * @param startPos the index (zero based) where to start the substring from, if negative it's counted from end
     * @param len      length of the desired substring, if longer then the rest of the string, all the remaining characters are taken
     * @return column with requested substring
     */
    def zeroBasedSubstr(startPos: Int, len: Int): Column = {
      if (startPos >= 0) {
        column.substr(startPos + 1, len)
      } else {
        val startPosColumn = greatest(length(column) + startPos + 1, lit(1))
        val lenColumn = lit(len) + when(length(column) + startPos <= 0, length(column) + startPos).otherwise(0)
        column.substr(startPosColumn, lenColumn)
      }
    }
  }
}
