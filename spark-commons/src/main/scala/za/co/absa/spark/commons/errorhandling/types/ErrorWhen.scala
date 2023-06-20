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

package za.co.absa.spark.commons.errorhandling.types

import org.apache.spark.sql.Column
import za.co.absa.spark.commons.errorhandling.ErrorMessageSubmit

/**
 * A case class that puts together an error specification and the condition to identify it.
 * The primary usage is in [[za.co.absa.spark.commons.errorhandling.ErrorHandling.putErrorsWithGrouping ErrorHandling.putErrorsWithGrouping()]]
 * @param when - boolean column expression that should evaluate to true on and only on the error detection
 * @param errorMessageSubmit - the error specification
 * @group Error Handling
 * @since 0.6.0
 */
case class ErrorWhen (
                     when: Column,
                     errorMessageSubmit: ErrorMessageSubmit
                     )
