package datavalidation.spark.utils

import java.io.File

import datavalidation.spark.dto.{Field, SchemaDTO}
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, file2JsonInput}

object SchemaParser {

  def parse(schemaFile: File)(implicit formats: DefaultFormats = DefaultFormats): SchemaDTO = {
    SchemaDTO(JsonMethods.parse(file2JsonInput(schemaFile)).extract[List[Field]])
  }
}
