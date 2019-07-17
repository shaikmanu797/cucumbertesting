package datavalidation.spark.tests

import java.io.File

import datavalidation.spark.utils.{DataFrameSuite, SchemaParser}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class BasicValidationTest extends DataFrameSuite {

  val expectedSchema: StructType = StructType.fromDDL("id Long, name String, age Int")
  var actualDF, expectedDF: DataFrame = _
  var actualSchema: StructType = _

  before {
    actualSchema = SchemaParser.parse(new File(this.getClass.getResource("/schemas/sample.json").getPath)).toStructType
    actualDF = sparkSession.emptyDataFrame
    expectedDF = sparkSession.emptyDataFrame
  }

  test("Empty DataFrames comparision") {
    assertDataFrameEquals(expectedDF, actualDF)
  }

  test("Validate schema from layout json file for spark StructType Compatibility") {
    assert(expectedSchema, actualSchema)
  }

  test("Invalid schema parsing exception for unknown datatype Number") {
    val caught: ParseException = intercept[ParseException](SchemaParser.parse(
      new File(this.getClass.getResource("/schemas/invalid.json").getPath)).toStructType
    )
    assert(caught.getMessage.contains("DataType number is not supported."))
  }

  test("Compare loaded DataFrames") {
    actualDF = sparkSession.read
      .option(key = "header", value = true)
      .schema(actualSchema)
      .csv(this.getClass.getResource("/data/sample.csv").getPath)

    expectedDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(Row(1L, "Tom", 24),
          Row(2L, "Jerry", 31),
          Row(3L, "Mickey", 13),
          Row(4L, "Minnie", 9),
          Row(5L, "Chopsy", 17)
        )
      ),
      expectedSchema
    )
    assertDataFrameEquals(expectedDF, actualDF)
  }
}
