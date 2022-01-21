# spark-commons


[![Build](https://github.com/AbsaOSS/spark-hats/workflows/Build/badge.svg)](https://github.com/AbsaOSS/spark-commons/actions)

sbt
```scala
libraryDependencies += "za.co.absa" %% "spark-commons" % "0.1.1"
```

### Scala 2.11 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.11)

Maven
```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-commons_2.11</artifactId>
   <version>0.1.1</version>
</dependency>
```

### Scala 2.12 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-commons_2.12)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-commons_2.11</artifactId>
   <version>0.1.1</version>
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
>Different _Scala_ variants of the _Schema Utils_ are compiled against different _Spark_, _Json4s_ and _Jackson_ versions:
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