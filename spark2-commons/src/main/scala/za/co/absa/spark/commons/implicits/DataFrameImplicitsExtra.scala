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
import org.apache.spark.sql.DataFrame
import za.co.absa.spark.commons.implicits.StructTypeImplicitsExtra.DataFrameSelector
import za.co.absa.spark.commons.implicits.DataFrameImplicits.{DataFrameEnhancements => BasicDataFrameEnhancements}

object DataFrameImplicitsExtra {

  implicit class DataFrameEnhancements(df: DataFrame) extends BasicDataFrameEnhancements(df){
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
