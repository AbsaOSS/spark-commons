# spark-commons


[![Build](https://github.com/AbsaOSS/spark-hats/workflows/Build/badge.svg)](https://github.com/AbsaOSS/spark-commons/actions)

sbt
```scala
libraryDependencies += "za.co.absa" %% "spark-commons" % "X.Y.Z"
```

### Scala 2.11 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.11)

Maven
```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-commons_2.11</artifactId>
   <version>${latest_version}</version>
</dependency>
```

### Scala 2.12 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.12)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-commons_2.12</artifactId>
   <version>${latest_version}</version>
</dependency>
```

# Spark Utils

### NonFatalQueryExecutionListenerAdapter

A trait that when is mixed with another `QueryExecutionListener` implementation, 
makes sure the later is not called with any fatal exception.   

See https://github.com/AbsaOSS/commons/issues/50

```scala
val myListener = new MyQueryExecutionListener with NonFatalQueryExecutionListenerAdapter
spark.listenerManager.register(myListener)
```

### Spark Schema Utils

>
>**Note:**
>Different _Scala_ variants of the _Schema Utils_ are compiled against different _Spark_, _Json4s_ and _Jackson_ versions.
>2.13 is not yet fully supported:
>
>| | Scala 2.11 | Scala 2.12 | Scala 2.13 | 
>|---|---|---|---|
>|Spark| 2.4 | 3.1 | 3.2 |
>|Json4s| 3.5 | 3.7 | 3.7 |
>|Jackson| 2.6 | 2.10 | 2.12 |
_Spark Schema Utils_ provides methods for working with schemas, its comparison and alignment.  

1. Schema comparison returning true/false. Ignores the order of columns

    ```scala
      SchemaUtils.equivalentSchemas(schema1, schema2)
    ```

2. Schema comparison returning difference. Ignores the order of columns

    ```scala
      SchemaUtils.diff(schema1, schema2)
    ```

3. Schema selector generator which provides a List of columns to be used in a 
select to order and positionally filter columns of a DataFrame

    ```scala
      SchemaUtils.getDataFrameSelector(schema)
    ```

4. Dataframe alignment method using the `getDataFrameSelector` method.

    ```scala
      SchemaUtils.alignSchema(dataFrameToBeAligned, modelSchema)
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

# Spark Version Guard

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