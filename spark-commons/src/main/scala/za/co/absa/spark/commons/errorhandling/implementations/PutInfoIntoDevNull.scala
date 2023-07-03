package za.co.absa.spark.commons.errorhandling.implementations

import org.apache.spark.sql.{Column, DataFrame}

object PutInfoIntoDevNull {

  def putErrorsToNullTypeColumn(dataframe: DataFrame)(errCols: Column): DataFrame = {

    dataframe
  }

}
