package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, collect_list}
import za.co.absa.spark.commons.errorhandling.{ErrorMessageSubmit}
import za.co.absa.spark.commons.errorhandling.partials.ErrorHandlingCommon
import za.co.absa.spark.commons.errorhandling.types.ErrorColumn

object ErrorHandlingFilterRowsWithErrors extends ErrorHandlingCommon {
  override def aggregateErrorColumns(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame = {
    val aggregatedDF = dataFrame.groupBy("errCode")
      .agg(coalesce(collect_list("errCols")) as "AggregatedError")
    aggregatedDF.filter(!col("AggregatedError"))
  }

  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = ???

  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = ???
}
