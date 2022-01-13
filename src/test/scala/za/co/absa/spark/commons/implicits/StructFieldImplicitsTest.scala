package za.co.absa.spark.commons.implicits

import org.apache.spark.sql.types.{Metadata, StringType, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldEnhancements

class StructFieldImplicitsTest extends AnyFunSuite {

  def fieldWith(value123: String) = {
    val value1 = s"""{ \"a\" : ${value123} }"""
    StructField("uu", StringType, true, Metadata.fromJson(value1))
  }

  test("getMetadataString") {
    assertResult(Some(""))(fieldWith("\"\"").getMetadataString("a"))
    assertResult(None)(fieldWith("123").getMetadataString("a"))
    assertResult(Some("ffbfg"))(fieldWith("\"ffbfg\"").getMetadataString("a"))
    assertResult(Some(null))(fieldWith("null").getMetadataString("a"))
  }

  test("getMetadataChar") {
    assertResult(None)(fieldWith("\"\"").getMetadataChar("a"))
    assertResult(None)(fieldWith("123").getMetadataChar("a"))
    assertResult(Some('g'))(fieldWith("\"g\"").getMetadataChar("a"))
    assertResult(None)(fieldWith("null").getMetadataChar("a"))
  }

  test("getMetadataStringAsBoolean") {
    assertResult(None)(fieldWith("\"\"").getMetadataStringAsBoolean("a"))
    assertResult(None)(fieldWith("123").getMetadataStringAsBoolean("a"))
    assertResult(Some(true))(fieldWith("\"true\"").getMetadataStringAsBoolean("a"))
    assertResult(Some(false))(fieldWith("\"false\"").getMetadataStringAsBoolean("a"))
    assertResult(None)(fieldWith("false").getMetadataStringAsBoolean("a"))
    assertResult(None)(fieldWith("true").getMetadataStringAsBoolean("a"))
    assertResult(None)(fieldWith("null").getMetadataStringAsBoolean("a"))
  }

  test("hastMetadataKKey") {
    assertResult(true)(fieldWith("\"\"").hasMetadataKey("a"))
    assertResult(false)(fieldWith("123").hasMetadataKey("b"))
    assertResult(true)(fieldWith("\"hvh\"").hasMetadataKey("a"))
    assertResult(true)(fieldWith("null").hasMetadataKey("a"))
  }

}
