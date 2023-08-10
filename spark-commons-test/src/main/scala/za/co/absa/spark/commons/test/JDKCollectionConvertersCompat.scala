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

package za.co.absa.spark.commons.test

// Solution based on conversation in scala-collection-compat
// https://github.com/scala/scala-collection-compat/issues/208
object JDKCollectionConvertersCompat {
  private object ScopePre213 {
    object jdk {
      type CollectionConverters = Int
    }
  }
  import ScopePre213._

  private object ScopeFor213 {
    import scala.collection.{JavaConverters => CollectionConverters}
    object Inner {
      import scala._
      import jdk.CollectionConverters
      val Converters = CollectionConverters
    }
  }

  val Converters = ScopeFor213.Inner.Converters
}
