package za.co.absa.spark.commons.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object ColUtils {
  def col2Expr(c: Column): Expression = {
    c.expr
  }

  def expr2Col(e: Expression): Column = {
    new Column(e)
  }
}
