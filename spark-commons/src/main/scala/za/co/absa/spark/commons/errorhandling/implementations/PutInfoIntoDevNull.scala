package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.errorhandling.{ErrorHandling, ErrorMessageSubmit}

object PutInfoIntoDevNull extends ErrorHandling {

  /**
   * First of the few methods that needs to be coded in the trait implementation
   * The purpose of this method is to convert the error specification into a [[org.apache.spark.sql.Column spark.Column]] expression
   *
   * @param errorMessageSubmit - the error specification
   * @return - the error specification transformed into a column expression
   * @group Error Handling
   * @since 0.6.0
   */
  override protected def transformErrorSubmitToColumn(errorMessageSubmit: ErrorMessageSubmit): Column = ???

  /**
   * Applies the provided columns to the incoming [[org.apache.spark.sql.DataFrame spark.DataFrame]]. Usually they might be aggregated in some way and attached
   * to the [[org.apache.spark.sql.DataFrame spark.DataFrame]], but any other operations are imaginable. Unless really bent, the incoming columns are those
   * produced by [[transformErrorSubmitToColumn]].
   * The idea here is that the error column contains information of the error that occurred on the row or is empty (NULL)
   * otherwise.
   * In each implementation calling the function to each column separately or in any grouping of columns should produce
   * the same result (with the exception of order of errors in the aggregation).
   *
   * @param dataFrame - the [[org.apache.spark.sql.DataFrame spark.DataFrame]] to apply the error columns to
   * @param errCols   - the list of error columns to apply
   * @return - data frame with the error columns applied (aggregated and attached or done otherwise)
   */
  override protected def doApplyErrorColumnsToDataFrame(dataFrame: DataFrame, errCols: Column*): DataFrame = {
    dataFrame
  }

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandling]] is structured.
   * This function provides the information on the structure of single error column
   *
   * @return -  the DataType of the column returned from `createErrorAsColumn` function
   */
override def errorColumnType: DataType = ???

  /**
   * Provides the library some information about how the actual implementation of [[ErrorHandling]] is structured.
   * This function describes what is the type of the column attached (if it didn't exists before) to the [[org.apache.spark.sql.DataFrame DataFrame]]
   *
   * @return - the DataType of the column containing the error info that is attached to the [[org.apache.spark.sql.DataFrame DataFrame]].
   */
  override def dataFrameColumnType: Option[DataType] = ???
}
