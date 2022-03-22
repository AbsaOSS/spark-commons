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

package za.co.absa.spark.commons.implicits

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.implicits.StructTypeImplicits.DataFrameSelector

object DataFrameImplicits {

  implicit class DataFrameEnhancements(val df: DataFrame) {

    private def gatherData(showFnc: () => Unit): String = {
      val outCapture = new ByteArrayOutputStream
      Console.withOut(outCapture) {
        showFnc()
      }
      val dfData = new String(outCapture.toByteArray).replace("\r\n", "\n")
      dfData
    }

    def dataAsString(): String = {
      val showFnc: () => Unit = df.show
      gatherData(showFnc)
    }

    def dataAsString(truncate: Boolean): String = {
      val showFnc:  () => Unit = ()=>{df.show(truncate)}
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int, vertical: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate, vertical)
      gatherData(showFnc)
    }

    /**
      * Adds a column to a dataframe if it does not exist
      *
      * @param colName     A column to add if it does not exist already
      * @param ifExists    A function to apply when the column already exists
      * @param colExpr     An expression for the column to add
      * @return a new dataframe with the new column
      */
    def withColumnIfDoesNotExist(ifExists: (DataFrame, String) => DataFrame)(colName: String, colExpr: Column): DataFrame = {
      if (df.schema.exists(field => field.name.equalsIgnoreCase(colName))) {
        ifExists(df, colName)
      } else {
        df.withColumn(colName, colExpr)
      }
    }

    /**
     * Using utils selector  aligns the utils of a DataFrame to the selector
     * for operations where utils order might be important (e.g. hashing the whole rows and using except)
     *
     * @param selector model structType for the alignment of df
     * @return Returns aligned and filtered utils
     */
    def alignSchema(selector: List[Column]): DataFrame = df.select(selector: _*)

    /**
     * Using utils selector from [[DataFrameSelector.getDataFrameSelector]] aligns the utils of a DataFrame to the selector for operations
     * where utils order might be important (e.g. hashing the whole rows and using except)
     *
     * @param structType model structType for the alignment of df
     * @return Returns aligned and filtered utils
     */
    def alignSchema(structType: StructType): DataFrame = {
      alignSchema(structType.getDataFrameSelector())
    }
  }

}
