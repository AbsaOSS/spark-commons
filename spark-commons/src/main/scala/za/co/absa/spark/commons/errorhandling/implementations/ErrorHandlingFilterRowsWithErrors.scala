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

//    val columnCheck: Array[Boolean] = errorMessageSubmit.map(_ => false).toArray
//    val columnCheck: Array[Boolean] = Array.map(errorMessageSubmit.errColsAndValues)(false)
    val columnCheck: Array[Boolean] = List.fill(errorMessageSubmit.errMsg)(true)
  }

  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregatedDF = dataFrame.groupBy("errCode")
      .agg(coalesce(collect_list("errCols")) as "AggregatedError")
    aggregatedDF.filter(!col("AggregatedError"))
  }

}
