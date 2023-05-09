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

import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{ArrayType, NullType, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.commons.adapters.TransformAdapter
import za.co.absa.spark.commons.implicits.StructTypeImplicits.DataFrameSelector

import java.io.ByteArrayOutputStream

object DataFrameImplicits {

  implicit class DataFrameEnhancements(val df: DataFrame) extends TransformAdapter {

    private def gatherData(showFnc: () => Unit): String = {
      val outCapture = new ByteArrayOutputStream
      Console.withOut(outCapture) {
        showFnc()
      }
      val dfData = new String(outCapture.toByteArray).replace("\r\n", "\n")
      dfData
    }

    //TODO  Fix ScalaDoc cross-module links #48 - Dataset.show()
    /**
     * Get the string representation of the data in the format as [[org.apache.spark.sql.DataFrame spark.DataFrame]] displays them
     *
     * @return  The string representation of the data in the DataFrame
     * @since 0.2.0
     */
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
     * Casts NullType (aka VOID) fields to their target types as defined in `targetSchema`.
     *
     * Matching of fields is "by name", so order of fields in schema(s) doesn't matter.
     * Resulting DataFrame has the same fields order as original DataFrame.
     * All fields from original DataFrame are kept in resulting DataFrame, even those that are not in `targetSchema`.
     *
     * @param targetSchema definition of field types to which potential NullTypes will be casted to
     * @return DataFrame with fields of NullType casted to their type in `targetSchema`
     */
    def enforceTypeOnNullTypeFields(targetSchema: StructType): DataFrame = {

      def processArray(
                        thisArrType: ArrayType, targetArrType: ArrayType, thisArrayColumn: Column
                      ): Column =
        (thisArrType.elementType, targetArrType.elementType) match {
          case (_: NullType, _: NullType) => thisArrayColumn
          case (_: NullType, _) =>
            transform(
              thisArrayColumn,
              _ => lit(null).cast(targetArrType.elementType)
            )
          case (thisNestedArrType: ArrayType, targetNestedArrType: ArrayType) =>
            transform(
              thisArrayColumn,
              processArray(thisNestedArrType, targetNestedArrType, _)
            )
          case (thisNestedStructType: StructType, targetNestedStructType: StructType) =>
            transform(
              thisArrayColumn,
              element => struct(processStruct(thisNestedStructType, targetNestedStructType, Some(element)): _*)
            )
          case _ => thisArrayColumn
        }

      def processStruct(
                         currentThisSchema: StructType, currentTargetSchema: StructType, parent: Option[Column]
                       ): List[Column] = {
        val currentTargetSchemaMap = currentTargetSchema.map(f => (f.name.toLowerCase, f)).toMap

        currentThisSchema.foldRight(List.empty[Column])((field, acc) => {
          val currentColumn: Column = parent
            .map(_.getField(field.name))
            .getOrElse(col(field.name))
          val correspondingTargetType = currentTargetSchemaMap.get(field.name.toLowerCase).map(_.dataType)

          val castedColumn = (field.dataType, correspondingTargetType) match {
            case (NullType, Some(NullType)) => currentColumn
            case (NullType, Some(targetType)) => currentColumn.cast(targetType)
            case (arrType: ArrayType, Some(targetArrType: ArrayType)) =>
              processArray(arrType, targetArrType, currentColumn)
            case (structType: StructType, Some(targetStructType: StructType)) =>
              struct(processStruct(structType, targetStructType, Some(currentColumn)): _*)
            case _ => currentColumn
          }
          castedColumn.as(field.name) :: acc
        })
      }

      val thisSchema = df.schema
      val selector = processStruct(thisSchema, targetSchema, None)
      df.select(selector: _*)
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
     * Using utils selector from [[StructTypeImplicits.DataFrameSelector.getDataFrameSelector]] aligns the utils of a DataFrame to the selector for operations
     * where utils order might be important (e.g. hashing the whole rows and using except)
     *
     * @param structType model structType for the alignment of df
     * @return Returns aligned and filtered utils
     */
    def alignSchema(structType: StructType): DataFrame = {
      alignSchema(structType.getDataFrameSelector())
    }

    //TODO  Fix ScalaDoc cross-module links #48 - Dataset.cache()
    /**
     * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`), the same way as
     * Dataset.cache does. But does not throw a warning if the DataFrame has been cached before.
     *
     * @return the DataFrame itself
     * @since 0.3.0
     */
    def cacheIfNotCachedYet(): DataFrame = {
      val planToCache = df.queryExecution.analyzed
      if (df.sparkSession.sharedState.cacheManager.lookupCachedData(planToCache).isEmpty) {
        df.cache()
      } else {
        df
      }
    }
  }

}
