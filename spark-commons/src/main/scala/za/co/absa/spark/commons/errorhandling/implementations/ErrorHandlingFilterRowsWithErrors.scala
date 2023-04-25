package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, lit}
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
    doTheColumnsAggregation(dataFrame, errCols.map(_.column): _*)
  }

  /**
   * Creates a column with the error description, in this particular case actually only signals with a boolean flag there was an error in the row.
   * @param errorMessageSubmit the object that defines the structure of the column
   * @return returns true if the column contains an error
   */
  override protected def evaluate(errorMessageSubmit: ErrorMessageSubmit): Column = {
    lit(true)
  }

  /**
   * Filters out the rows where the first non-null values selected columns when it is false or null
   * @param dataFrame the overall data structure that need to be aggregated
   * @param errCols the columns to aggregate the dataframe with
   * @return Returns aggregated dataset with errors.
   */
  override protected def doTheColumnsAggregation(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    val columns: Seq[Column] = errCols :+ lit(false)
    dataFrame.filter(!coalesce(columns: _*).isNull)
  }

}
