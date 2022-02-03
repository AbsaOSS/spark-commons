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

package za.co.absa.spark.commons.schema

import org.apache.spark.sql.types._
import za.co.absa.spark.commons.adapters.HofsAdapter


object SchemaUtils extends HofsAdapter {

  /**
   * Returns the parent path of a field. Returns an empty string if a root level field name is provided.
   *
   * @param columnName A fully qualified column name
   * @return The parent column name or an empty string if the input column is a root level column
   */
  def getParentPath(columnName: String): String = {
    val index = columnName.lastIndexOf('.')
    if (index > 0) {
      columnName.substring(0, index)
    } else {
      ""
    }
  }

  /**
   * Compares 2 array fields of a dataframe schema.
   *
   * @param array1 The first array to compare
   * @param array2 The second array to compare
   * @return true if provided arrays are the same ignoring nullability
   */
  @scala.annotation.tailrec
  private def equalArrayTypes(array1: ArrayType, array2: ArrayType): Boolean = {
    array1.elementType match {
      case arrayType1: ArrayType =>
        array2.elementType match {
          case arrayType2: ArrayType => equalArrayTypes(arrayType1, arrayType2)
          case _ => false
        }
      case structType1: StructType =>
        array2.elementType match {
          case structType2: StructType => equivalentSchemas(structType1, structType2)
          case _ => false
        }
      case _ => array1.elementType == array2.elementType
    }
  }

  /**
   * Finds all differences of two ArrayTypes and returns their paths
   *
   * @param array1 The first array to compare
   * @param array2 The second array to compare
   * @param parent Parent path. This is used for the accumulation of differences and their print out
   * @return Returns a Seq of found difference paths in scheme in the Array
   */
  @scala.annotation.tailrec
  private def diffArray(array1: ArrayType, array2: ArrayType, parent: String): Seq[String] = {
    array1.elementType match {
      case _ if array1.elementType.typeName != array2.elementType.typeName =>
        Seq(s"$parent data type doesn't match (${array1.elementType.typeName}) vs (${array2.elementType.typeName})")
      case arrayType1: ArrayType =>
        diffArray(arrayType1, array2.elementType.asInstanceOf[ArrayType], s"$parent")
      case structType1: StructType =>
        diffSchema(structType1, array2.elementType.asInstanceOf[StructType], s"$parent")
      case _ => Seq.empty[String]
    }
  }

  /**
   * Compares 2 fields of a dataframe schema.
   *
   * @param type1 The first field to compare
   * @param type2 The second field to compare
   * @return true if provided fields are the same ignoring nullability
   */
  private def equivalentTypes(type1: DataType, type2: DataType): Boolean = {
    type1 match {
      case arrayType1: ArrayType =>
        type2 match {
          case arrayType2: ArrayType => equalArrayTypes(arrayType1, arrayType2)
          case _ => false
        }
      case structType1: StructType =>
        type2 match {
          case structType2: StructType => equivalentSchemas(structType1, structType2)
          case _ => false
        }
      case _ => type1 == type2
    }
  }

  /**
   * Determine if a datatype is a primitive one
   */
  def isPrimitive(dt: DataType): Boolean = dt match {
    case _: BinaryType
         | _: BooleanType
         | _: ByteType
         | _: DateType
         | _: DecimalType
         | _: DoubleType
         | _: FloatType
         | _: IntegerType
         | _: LongType
         | _: NullType
         | _: ShortType
         | _: StringType
         | _: TimestampType => true
    case _ => false
  }

  /**
   * Finds all differences of two StructFields and returns their paths
   *
   * @param field1 The first field to compare
   * @param field2 The second field to compare
   * @param parent Parent path. This is used for the accumulation of differences and their print out
   * @return Returns a Seq of found difference paths in scheme in the StructField
   */
  private def diffField(field1: StructField, field2: StructField, parent: String): Seq[String] = {
    field1.dataType match {
      case _ if field1.dataType.typeName != field2.dataType.typeName =>
        Seq(s"$parent.${field1.name} data type doesn't match (${field1.dataType.typeName}) vs (${field2.dataType.typeName})")
      case arrayType1: ArrayType =>
        diffArray(arrayType1, field2.dataType.asInstanceOf[ArrayType], s"$parent.${field1.name}")
      case structType1: StructType =>
        diffSchema(structType1, field2.dataType.asInstanceOf[StructType], s"$parent.${field1.name}")
      case _ =>
        Seq.empty[String]
    }
  }

  /**
   * Compares 2 dataframe schemas.
   *
   * @param schema1 The first schema to compare
   * @param schema2 The second schema to compare
   * @return true if provided schemas are the same ignoring nullability
   */
  def equivalentSchemas(schema1: StructType, schema2: StructType): Boolean = {
    val fields1 = schema1.sortBy(_.name.toLowerCase)
    val fields2 = schema2.sortBy(_.name.toLowerCase)

    fields1.size == fields2.size &&
      fields1.zip(fields2).forall {
        case (f1, f2) =>
          f1.name.equalsIgnoreCase(f2.name) &&
            equivalentTypes(f1.dataType, f2.dataType)
      }
  }

  /**
   * Returns a list of differences in one schema to the other
   *
   * @param schema1 The first schema to compare
   * @param schema2 The second schema to compare
   * @param parent  Parent path. Should be left default by the users first run. This is used for the accumulation of
   *                differences and their print out.
   * @return Returns a Seq of paths to differences in schemas
   */
  def diffSchema(schema1: StructType, schema2: StructType, parent: String = ""): Seq[String] = {
    val fields1 = getMapOfFields(schema1)
    val fields2 = getMapOfFields(schema2)

    val diff = fields1.values.foldLeft(Seq.empty[String])((difference, field1) => {
      val field1NameLc = field1.name.toLowerCase()
      if (fields2.contains(field1NameLc)) {
        val field2 = fields2(field1NameLc)
        difference ++ diffField(field1, field2, parent)
      } else {
        difference ++ Seq(s"$parent.${field1.name} cannot be found in both schemas")
      }
    })

    diff.map(_.stripPrefix("."))
  }

  /**
   * Checks if the originalSchema is a subset of subsetSchema.
   *
   * @param subsetSchema   The schema that needs to be extracted
   * @param originalSchema The schema that needs to have at least all t
   * @return true if provided schemas are the same ignoring nullability
   */
  def isSubset(subsetSchema: StructType, originalSchema: StructType): Boolean = {
    val subsetFields = getMapOfFields(subsetSchema)
    val originalFields = getMapOfFields(originalSchema)

    subsetFields.forall(subsetField =>
      originalFields.contains(subsetField._1) &&
        equivalentTypes(subsetField._2.dataType, originalFields(subsetField._1).dataType))
  }

  private def getMapOfFields(schema: StructType): Map[String, StructField] = {
    schema.map(field => field.name.toLowerCase() -> field).toMap
  }

  /**
   * Get paths for all array subfields of this given datatype
   */
  def getAllArraySubPaths(path: String, name: String, dt: DataType): Seq[String] = {
    val currPath = appendPath(path, name)
    dt match {
      case s: StructType => s.fields.flatMap(f => getAllArraySubPaths(currPath, f.name, f.dataType))
      case _@ArrayType(elType, _) => getAllArraySubPaths(path, name, elType) :+ currPath
      case _ => Seq()
    }
  }

  /**
   * For a given list of field paths determines if any path pair is a subset of one another.
   *
   * For instance,
   *  - 'a.b', 'a.b.c', 'a.b.c.d' have this property.
   *  - 'a.b', 'a.b.c', 'a.x.y' does NOT have it, since 'a.b.c' and 'a.x.y' have diverging subpaths.
   *
   * @param paths A list of paths to be analyzed
   * @return true if for all pathe the above property holds
   */
  def isCommonSubPath(paths: String*): Boolean = {
    def sliceRoot(paths: Seq[Seq[String]]): Seq[Seq[String]] = {
      paths.map(path => path.drop(1)).filter(_.nonEmpty)
    }

    var isParentCommon = true // For Seq() the property holds by [my] convention
    var restOfPaths: Seq[Seq[String]] = paths.map(_.split('.').toSeq).filter(_.nonEmpty)
    while (isParentCommon && restOfPaths.nonEmpty) {
      val parent = restOfPaths.head.head
      isParentCommon = restOfPaths.forall(path => path.head == parent)
      restOfPaths = sliceRoot(restOfPaths)
    }
    isParentCommon
  }

  /**
   * Append a new attribute to path or empty string.
   *
   * @param path      The dot-separated existing path
   * @param fieldName Name of the field to be appended to the path
   * @return The path with the new field appended or the field itself if path is empty
   */
  def appendPath(path: String, fieldName: String): String = {
    if (path.isEmpty) {
      fieldName
    } else if (fieldName.isEmpty) {
      path
    } else {
      s"$path.$fieldName"
    }
  }


  /**
   * Checks if a casting between types always succeeds
   *
   * @param sourceType A type to be casted
   * @param targetType A type to be casted to
   * @return true if casting never fails
   */
  def doesCastAlwaysSucceed(sourceType: DataType, targetType: DataType): Boolean = {
    (sourceType, targetType) match {
      case (_: StructType, _) | (_: ArrayType, _) => false
      case (a, b) if a == b => true
      case (_, _: StringType) => true
      case (_: ByteType, _: ShortType | _: IntegerType | _: LongType) => true
      case (_: ShortType, _: IntegerType | _: LongType) => true
      case (_: IntegerType, _: LongType) => true
      case (_: DateType, _: TimestampType) => true
      case _ => false
    }
  }
}
