package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, collect_list}
import za.co.absa.spark.commons.errorhandling.{ErrorMessageSubmit}
import za.co.absa.spark.commons.errorhandling.partials.ErrorHandlingCommon
import za.co.absa.spark.commons.errorhandling.types.ErrorColumn

/**
 *
 */

object ErrorHandlingFilterRowsWithErrors extends ErrorHandlingCommon {
  override def aggregateErrorColumns(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame = {
    register(dataFrame.sparkSession)
    doTheAggregation(dataFrame, errCols.map(_.column): _*)
  }

  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
//    val colCheck: Array[Boolean] = errorMessageSubmit.fill(errorColumn =>false).toArray()
    val columnCheck: Array[Boolean] = Array.fill(errorMessageSubmit.errColsAndValues)(false)
    columnCheck
  }

  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregatedDF = dataFrame.groupBy("errCode")
      .agg(coalesce(collect_list("errCols")) as "AggregatedError")
    aggregatedDF.filter(!col("AggregatedError"))
  }

}
