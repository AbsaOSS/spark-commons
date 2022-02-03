package za.co.absa.spark.commons.implicits

import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.annotation.tailrec

object ArrayTypeImplicits {

  implicit class ArrayTypeEnhancements(arrayType: ArrayType) {

    /**
     * For an array of arrays of arrays, ... get the final element type at the bottom of the array
     *
     * @return A non-array data type at the bottom of array nesting
     */
    final def getDeepestArrayType(): Unit = {
      @tailrec
      def getDeepestArrayTypeHelper(arrayType: ArrayType): DataType = {
        arrayType.elementType match {
          case a: ArrayType => getDeepestArrayTypeHelper(a)
          case b => b
        }
      }
      getDeepestArrayTypeHelper(arrayType)
    }


  }
}
