package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, lit}
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit
import za.co.absa.spark.commons.errorhandling.partials.ErrorHandlingCommon

/**
 * Class implement the functionality of filtering rows with columns.
 */

object ErrorHandlingFilterRowsWithErrors extends ErrorHandlingCommon {

  /**
   * Creates a column with the error description, in this particular case actually only signals with a boolean flag there was an error in the row.
   * @param errorMessageSubmit - the description of the error
   * @return returns true if the column contains an error
   */
  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
    lit(true)
  }

  /**
   * Filters out rows that contain at least one true flag in the provided boolean error columns
   * @param dataFrame the overall data structure that need to be aggregated
   * @param errCols - the error columns to signal if the row should be filtered or not
   * @return - returns the dataframe without rows with errors
   */
  override protected def doTheColumnsAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val columns: Seq[Column] = errCols :+ lit(false)
    dataFrame.filter(!coalesce(columns: _*).isNull)
  }

}
