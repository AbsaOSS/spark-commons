package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, collect_list}
import za.co.absa.spark.commons.errorhandling.{ErrorMessageSubmit}
import za.co.absa.spark.commons.errorhandling.partials.ErrorHandlingCommon
import za.co.absa.spark.commons.errorhandling.types.ErrorColumn

/**
 * Class implement the functionality of filtering rows with columns.
 */

object ErrorHandlingFilterRowsWithErrors extends ErrorHandlingCommon {
  /**
   * Method return the rows that has errors on aggregation
   * @param dataFrame  the overall data structure that need to be checked for rows with errors
   * @param errCols  a final case class that provide error columns
   * @return returns rows with errors
   */
  override def aggregateErrorColumns(dataFrame: DataFrame)(errCols: ErrorColumn*): DataFrame = {
    register(dataFrame.sparkSession)
    doTheAggregation(dataFrame, errCols.map(_.column): _*)
  }

  /**
   * Evaluate the given column to check if it has errors
   * @param errorMessageSubmit the object that has to be evaluated for error purposes
   * @return returns the columns with error
   */
  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
    errorMessageSubmit.errMsg.column
  }

  /**
   * Checks for relationship of the provided clumn in the given dataframe.
   * @param dataFrame the overall data structure that need to be aggregated
   * @param errCols the row to aggregate the dataframe with
   * @return Returns the aggregated dataset with errors.
   */
  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregatedDF = dataFrame.groupBy("errCode")
      .agg(coalesce(collect_list("errCols")) as "AggregatedError")
    aggregatedDF.filter(!col("AggregatedError"))
  }

}
