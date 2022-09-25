# spark-commons

[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Build](https://github.com/AbsaOSS/spark-commons/actions/workflows/build.yml/badge.svg)](https://github.com/AbsaOSS/spark-commons/actions/workflows/build.yml)
[![Release](https://github.com/AbsaOSS/spark-commons/actions/workflows/release.yml/badge.svg)](https://github.com/AbsaOSS/spark-commons/actions/workflows/release.yml)

`spark-commons` is a library offering commonly needed routines, classes and functionality. It consists of three modules.
* spark-commons-spark2.4
* spark-commons-spark3.7
* spark-commons-test

**spark2-commons** and **spark3-commons** both offer the same logic for the respective major versions of Spark addressing
usual needs of Spark applications.

**spark-commons-test** then brings routines to help in testing Spark applications (and it's independent of Spark 
version used) 


|              | spark-commons-spark2.4                                                                                                                                                                        | spark-commons-spark3.2                                                                                                                                                                                         | spark-commons-test                                                                                                                                                                                     |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| _Scala 2.11_ | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark2.4_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark2.4_2.11) |                                                                                                                                                                                                                | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-test_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-test_2.11) | 
| _Scala 2.12_ | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark2.4_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark2.4_2.12)  | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark3.2_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-spark3.2_2.12) | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-test_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons-test_2.12) | 

## Spark-Commons

### NonFatalQueryExecutionListenerAdapter

A trait that when is mixed with another `QueryExecutionListener` implementation, 
makes sure the later is not called with any fatal exception.   

See https://github.com/AbsaOSS/commons/issues/50

```scala
val myListener = new MyQueryExecutionListener with NonFatalQueryExecutionListenerAdapter
spark.listenerManager.register(myListener)
```

### TransformAdapter

A trait that brings Spark version independent implementation of `transform` function.

### SchemaUtils

_SchemaUtils_ provides methods for working with schemas, its comparison and alignment.  

1. Returns the parent path of a field. Returns an empty string if a root level field name is provided.

    ```scala
      SchemaUtils.getParentPath(columnName)
    ```

2. Get paths for all array subfields of this given datatype

    ```scala
      SchemaUtils.getAllArraySubPaths(other)
    ```

3. For a given list of field paths determines if any path pair is a subset of one another.

    ```scala
      SchemaUtils.isCommonSubPath(paths)
    ```

4. Append a new attribute to path or empty string.

    ```scala
      SchemaUtils.appendPath(path, fieldName)
    ```

5. Separates the field name components of a fully qualified column name as they hierachy goes from root down to the
deepest one.

    ```scala
      SchemaUtils.splitPath(path, fieldName)
    ```


### JsonUtils

_Json Utils_ provides methods for working with Json, both on input and output.

1. Create a Spark DataFrame from a JSON document(s).

    ```scala
      JsonUtils.getDataFrameFromJson(json)
      JsonUtils.getDataFrameFromJson(json, schema)(implicit spark)
    ```

2. Creates a Spark Schema from a JSON document(s).

    ```scala
      JsonUtils.getSchemaFromJson(json)
    ```
   
### ColumnImplicits

_ColumnImplicits_ provide implicit methods for transforming Spark Columns

1. Transforms the column into a booleaan column, checking if values are negative or positive infinity

    ```scala
      column.isInfinite()
    ```
2. Returns column with requested substring. It shifts the substring indexation to be in accordance with Scala/ Java. 
    The provided starting position where to start the substring from, if negative it will be counted from end

    ```scala
      column.zeroBasedSubstr(startPos)
    ```
   
3. Returns column with requested substring. It shifts the substring indexation to be in accordance with Scala/ Java. 
   If the provided starting position where to start the substring from is negative, it will be counted from end. 
   The length of the desired substring, if longer then the rest of the string, all the remaining characters are taken.

    ```scala
      column.zeroBasedSubstr(startPos, length)
    ```

### StructFieldImplicits

_StructFieldImplicits_ provides implicit methods for working with StructField objects.  

Of them, metadata methods are:

1. Gets the metadata Option[String] value given a key

    ```scala
      structField.metadata.getOptString(key)
    ```
   
2. Gets the metadata Char value given a key if the value is a single character String, it returns the char,
 otherwise None

    ```scala
      structField.metadata.getOptChar(key)
    ```
  
3. Gets the metadata boolean value of a given key, given that it can be transformed into boolean

    ```scala
      structField.metadata.getStringAsBoolean(key)
    ```

4. Checks the structfield if it has the provided key, returns a boolean

    ```scala
      structField.metadata.hasKey(key)
    ```
   
### ArrayTypeImplicits

_ArrayTypeImplicits_ provides implicit methods for working with ArrayType objects.  


1. Checks if the arraytype is equivalent to another

    ```scala
      arrayType.isEquivalentArrayType(otherArrayType)
    ```   

2. For an array of arrays, get the final element type at the bottom of the array

    ```scala
      arrayType.getDeepestArrayType()
    ```   
   
### DataTypeImplicits

_DataTypeImplicits_ provides implicit methods for working with DataType objects.  


1. Checks if the datatype is equivalent to another

    ```scala
      dataType.isEquivalentDataType(otherDt)
    ```   

2. Checks if a casting between types always succeeds

    ```scala
      dataType.doesCastAlwaysSucceed(otherDt)
    ```   
3. Checks if type is primitive

    ```scala
      dataType.isPrimitive()
    ```
   
### StructTypeImplicits

_StructTypeImplicits_ provides implicit methods for working with StructType objects.  


1. Get a field from a text path

    ```scala
      structType.getField(path)
    ```
2. Get a type of a field from a text path

    ```scala
      structType.getFieldType(path)
    ```
3. Checks if the specified path is an array of structs

    ```scala
      structType.isColumnArrayOfStruct(path)
    ```

4. Get nullability of a field from a text path

    ```scala
      structType.getFieldNullability(path)
    ```

5. Checks if a field specified by a path exists

    ```scala
      structType.fieldExists(path)
    ```
    
6. Get paths for all array fields in the schema

    ```scala
      structType.getAllArrayPaths()
    ```
    
7. Get a closest unique column name

    ```scala
      structType.getClosestUniqueName(desiredName)
    ```

8. Checks if a field is the only field in a struct

    ```scala
      structType.isOnlyField(columnName)
    ```
9. Checks if 2 structtypes are equivalent

    ```scala
      structType.isEquivalent(other)
    ```

10. Returns a list of differences in one utils to the other

    ```scala
      structType.diffSchema(otherSchema, parent)
    ```

11. Checks if a field is of the specified type

    ```scala
      structType.isOfType[ArrayType](path)
    ```
12. Checks if a field is  a subset of the specified type

    ```scala
          structType.isSubset(other)
     ```
    
13. Returns data selector that can be used to align utils of a data frame.

    ```scala
          structType.getDataFrameSelector()
    ```
    
### StructTypeArrayImplicits

1. Get first array column's path out of complete path

    ```scala
      structType.getFirstArrayPath(path)
    ```
   
2. Get all array columns' paths out of complete path.

    ```scala
      structType.getAllArraysInPath(path)
    ```
   
3. For a given list of field paths determines the deepest common array path

    ```scala
      structType.getDeepestCommonArrayPath(fieldPaths)
    ```

4. For a field path determines the deepest array path

    ```scala
      structType.getDeepestArrayPath(path)
    ```
   
5. Checks if a field is an array that is not nested in another array

    ```scala
      structType.isNonNestedArray(path)
    ```

### DataFrameImplicits

1. Changes the fields structure of the DataFrame to adhere to the provided schema or selector. Data types remain intact

 ```scala
   dataFrame.alignSchema
 ```

2. Persist this Dataset with the default storage level, avoiding the warning in case the cache has happened already
   before

 ```scala
   dataFrame.cacheIfNotCachedYet()
 ```

3. Get the string representation of the data in the format as `Dataset.show()`]]` displays them

 ```scala
   dataFrame.dataAsString()
 ```

4. Adds a column to a dataframe if it does not exist

 ```scala
   dataFrame.withColumnIfDoesNotExist(path)
 ```



### Spark Version Guard

A class which checks if the Spark job version is compatible with the Spark Versions supported by the library

Default mode checking
```scala
SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)
```

Checking for 2.X versions
```scala
SparkVersionGuard.fromSpark2XCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)
```

Checking for 3.X versions
```scala
SparkVersionGuard.fromSpark3XCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)
```

### DataFrameImplicits
_DataFrameImplicits_ provides methods for transformations on Dataframes  

1. Getting the string of the data of the dataframe in similar fashion as the `show` function present them.

    ```scala
          df.dataAsString() 
      
          df.dataAsString(truncate)
      
          df.dataAsString(numRows, truncate)
   
          df.dataAsString(numRows, truncateNumber)
      
          df.dataAsString(numRows, truncate, vertical)
    ```
    
2. Adds a column to a dataframe if it does not exist. If it exists, it will apply the provided function
    
   ```scala
      df.withColumnIfDoesNotExist((df: DataFrame, _) => df)(colName, colExpression)
   ```

3. Aligns the utils of a DataFrame to the selector for operations
   where utils order might be important (e.g. hashing the whole rows and using except)

   ```scala
      df.alignSchema(structType)
   ```
   
   ```scala
      df.alignSchema(listColumns)
   ```
   
## Spark Commons Test

### Usage:

```scala
class MyTest extends SparkTestBase {
}
```

By default, it will instantiate a local Spark.
There is also the possibility to use it in yarn mode:

```scala
class MyTest extends SparkTestBase {
override lazy val spark: SparkSession = initSpark(new YarnSparkConfiguration(confDir, distJarsDir))
}
```

## How to Release

Please see [this file](RELEASE.md) for more details.
