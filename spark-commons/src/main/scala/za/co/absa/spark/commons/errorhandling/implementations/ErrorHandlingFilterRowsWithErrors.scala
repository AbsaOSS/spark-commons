package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
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
   * Checks if given column has errors or not
   * @param errorMessageSubmit the object that defines the structure of the column
   * @return returns true if the column contains an error
   */
  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
    lit(true)
  }

  /**
   * Checks for relationship of the provided column in the given dataframe.
   * @param dataFrame the overall data structure that need to be aggregated
   * @param errCols the columns to aggregate the dataframe with
   * @return Returns the aggregated dataset with errors.
   */
  override protected def doTheAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val aggregatedDF = dataFrame.groupBy("errCode")
      .agg(coalesce(errCols:_*, lit(false)) as "AggregatedError")
    aggregatedDF.filter(!col("AggregatedError"))
  }

}
