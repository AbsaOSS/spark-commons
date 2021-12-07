package za.co.absa.spark.commons

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.util.control.NonFatal

/**
 * Since Spark 2.4.6 a passed exception can be a wrapper on top of an Error.
 * This is a controversial decision that this trait is trying to revert
 *
 * @see https://issues.apache.org/jira/browse/SPARK-31144
 */
trait NonFatalQueryExecutionListenerAdapter extends QueryExecutionListener {

  abstract override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val causeOrItself = Option(exception.getCause).getOrElse(exception)

    if (NonFatal(causeOrItself))
      super.onFailure(funcName, qe, exception)
  }
}
