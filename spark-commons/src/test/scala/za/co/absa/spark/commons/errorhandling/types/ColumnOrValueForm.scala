/*
 * Copyright 2023 ABSA Group Limited
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

package za.co.absa.spark.commons.errorhandling.types

import org.apache.spark.sql.Column
import org.scalatest.Assertions

case class ColumnOrValueForm[T] (
                                 column: Column,
<<<<<<< HEAD
                                 isColumn: Boolean,
                                 isValue: Boolean,
=======
>>>>>>> master
                                 columnNames: Set[String],
                                 value: Option[T]
                               ) extends Assertions {
  def assertTo(columnOrValue: ColumnOrValue[T]): Unit ={
    assert(column == columnOrValue.column)
<<<<<<< HEAD
    assert(isColumn == columnOrValue.isColumn)
    assert(isValue == columnOrValue.isValue)
=======
>>>>>>> master
    assert(columnNames == columnOrValue.columnNames)
    assert(value == columnOrValue.getValue)
  }

}
