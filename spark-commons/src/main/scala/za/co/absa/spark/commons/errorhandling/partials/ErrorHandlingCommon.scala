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

package za.co.absa.spark.commons.errorhandling.partials

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.{ErrorHandling, ErrorMessageSubmit}
import za.co.absa.spark.commons.errorhandling.types._
import org.apache.spark.sql.functions.when

trait ErrorHandlingCommon extends ErrorHandling {
  protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column

  protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame

  def putErrorToColumn(errorMessageSubmit: ErrorMessageSubmit): ErrorColumn = {
    ErrorColumn(evaluate(errorMessageSubmit))
  }

  def aggregateErrorColumns(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame = {
    register(dataFrame.sparkSession)
    doTheAggregation(dataFrame, errCols.map(_.column): _*)
  }

  def putErrorsWithGrouping(dataFrame: DataFrame)(errorsWhen: Seq[ErrorWhen]): DataFrame = {
    register(dataFrame.sparkSession)
    val errorsByColumn = errorsWhen.groupBy(_.errorMessageSubmit.errColsAndValues.columnNames)
    val noColNames = Set.empty[String]
    val errorColumns1 = errorsByColumn.getOrElse(noColNames, Seq.empty).map(errorWhenToCol) // no grouping without ErrCol names
    val errorColumns2 = (errorsByColumn - noColNames).values.map(errorWhenSeqToCol).toSeq
    doTheAggregation(dataFrame, errorColumns1 ++ errorColumns2: _*)
  }


  private def errorWhenToCol(errorWhen: ErrorWhen): Column = {
    when(errorWhen.when, evaluate(errorWhen.errorMessageSubmit))
  }

  private def errorWhenSeqToCol(errorsWhen: Seq[ErrorWhen]): Column = {
    val branches: Seq[(Expression, Expression)] = errorsWhen.map(errorWhen => (errorWhen.when.expr, evaluate(errorWhen.errorMessageSubmit).expr))
    new Column(CaseWhen(branches))
  }

}
