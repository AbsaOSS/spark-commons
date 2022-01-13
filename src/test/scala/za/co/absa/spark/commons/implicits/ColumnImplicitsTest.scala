package za.co.absa.spark.commons.implicits

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.ColumnImplicits.ColumnEnhancements

class ColumnImplicitsTest extends AnyFunSuite{

  private val column: Column = lit("abcdefgh")

  test("zeroBasedSubstr with tartPos") {
    assertResult("cdefgh")(column.zeroBasedSubstr(2).expr.eval().toString)
    assertResult("gh")(column.zeroBasedSubstr(-2).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue).expr.eval().toString)
    assertResult("abcdefgh")(column.zeroBasedSubstr(Int.MinValue).expr.eval().toString)
  }

  test("zeroBasedSubstr with tartPos and len") {
    assertResult("cde")(column.zeroBasedSubstr(2, 3).expr.eval().toString)
    assertResult("gh")(column.zeroBasedSubstr(-2, 7).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue, 1).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MaxValue, -3).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MinValue,2).expr.eval().toString)
    assertResult("")(column.zeroBasedSubstr(Int.MinValue,-3).expr.eval().toString)
  }

}
