package datavalidation.spark.tests

import java.io.File
import java.util.Properties

import datavalidation.spark.utils.{DataFrameSuite, ParameterParser, SchemaParser}
import org.apache.spark.sql.DataFrame
import org.scalatest.DoNotDiscover
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConverters._

@DoNotDiscover
class FileComparisonTest extends DataFrameSuite {

  val resources: String = "resources:"
  val csvPrefix: String = "csv."
  var parameters: Properties = _
  var inputDF, outputDF: DataFrame = _

  before {
    parameters = ParameterParser.parse(new File(System.getProperty("parameter.file",
      this.getClass.getResource("/tests/filecomparison.properties").getPath)
    ))
    val csvProps: Map[String, String] = getCSVProps
    inputDF = loadCsvFile(getPropFile("input.file"), getPropFile("input.schema.file"), csvProps)
    outputDF = loadCsvFile(getPropFile("output.file"), getPropFile("output.schema.file"), csvProps)
  }

  test("Compare input vs output file data") {
    assertDataFrameEquals(inputDF, outputDF)
  }

  private def getCSVProps: Map[String, String] = {
    val csvProps: List[String] = parameters.stringPropertyNames()
      .asScala
      .filter(_.startsWith(csvPrefix))
      .toList
    csvProps.map(x => {
      x.replace(csvPrefix, "") -> parameters.getProperty(x)
    }).toMap
  }

  private def getPropFile(prop: String): File = {
    val fileProp: String = parameters.getProperty(prop)
    try {
      new File(if (fileProp.startsWith(resources)) {
        this.getClass.getResource(fileProp.replace(resources, "/")).getPath
      } else {
        fileProp
      })
    } catch {
      case ex: Exception =>
        throw new TestFailedException(s"File '$fileProp' not found given with property '$prop'!!", 10)
    }
  }

  private def loadCsvFile(file: File, schema: File, settings: Map[String, String] = Map.empty): DataFrame = {
    sparkSession.read
      .options(settings)
      .schema(SchemaParser.parse(schema).toStructType)
      .csv(file.getAbsolutePath)
  }
}
