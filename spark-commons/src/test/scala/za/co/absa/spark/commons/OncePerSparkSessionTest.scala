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

package za.co.absa.spark.commons

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import org.mockito.MockitoSugar

class OncePerSparkSessionTest extends AnyFunSuite with MockitoSugar with SparkTestBase {
  test("Each library is registered only once and exactly once per Spark session") {
    var libraryAInitCounter = 0
    var libraryBInitCounter = 0

    val anotherSpark: SparkSession =  mock[SparkSession]
    class UDFLibraryA()(implicit sparkToRegisterTo: SparkSession) extends OncePerSparkSession() {
      this.register(sparkToRegisterTo)
      override protected def registerBody(spark: SparkSession): Unit = {
        libraryAInitCounter += 1
      }
    }

    class UDFLibraryB()(implicit sparkToRegisterTo: SparkSession) extends OncePerSparkSession(sparkToRegisterTo) {
      override protected def registerBody(spark: SparkSession): Unit = {
        libraryBInitCounter += 1
      }
    }

    new UDFLibraryA()
    new UDFLibraryA()
    new UDFLibraryB()
    new UDFLibraryB()(anotherSpark)
    assert(libraryAInitCounter == 1)
    assert(libraryBInitCounter == 2)
  }

  test("should return true if the library is registered successfully and false if not and if spark is not hasn't started"){
    var libraryAInitCounter = 0
    var results_1 = false
    var results_2 = false
    class UDFLibrary()(implicit sparkToRegister: SparkSession) extends OncePerSparkSession(){
      results_1 = this.register(sparkToRegister)
      results_2 = this.register(sparkToRegister)
      override protected def registerBody(spark: SparkSession): Unit = {
        libraryAInitCounter += 1
      }
    }

    new UDFLibrary()
    assert(results_1 == true)
    assert(results_2 == false)
  }

  test("should return is registered successfully and false if not and if spark is not hasn't started") {
    var libraryAInitCounter = 0
//    var results_1 = false
//    var results_2 = false

    val anotherSpark: SparkSession =  mock[SparkSession]
    class UDFLibrary()(implicit sparkToRegisterTo: SparkSession) extends OncePerSparkSession(sparkToRegisterTo) {
      val results_1 = this.register(sparkToRegisterTo)
      val results_2 = this.register(sparkToRegisterTo)

      override protected def registerBody(spark: SparkSession): Unit = {
        libraryAInitCounter += 1
      }
    }

//    new UDFLibrary()
    val results = new UDFLibrary()(anotherSpark)
    assert(results.results_1 == true)
    assert(results.results_2 == false)
  }

}
