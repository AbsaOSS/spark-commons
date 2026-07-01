package za.co.absa.spark.commons.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ColumnConversions

object ColUtils {
  def col2Expr(c: Column): Expression = {
    ColumnConversions.expression(c)
  }

  def expr2Col(e: Expression): Column = {
    conversionFunctionE2C(e)
  }

  private val conversionFunctionE2C = {
    val clazz = Class.forName("org.apache.spark.sql.classic.ExpressionUtils$")
    val instance = clazz.getField("MODULE$").get(null)
    val method = clazz.getMethod("column", classOf[Expression])
    (expr: Expression) => method.invoke(instance, expr).asInstanceOf[Column]
  }

}
