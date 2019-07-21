package datavalidation.spark.utils

import java.io.File
import java.util.Properties

import scala.io.{BufferedSource, Source}

object ParameterParser {
  def parse(file: File): Properties = {
    val props: Properties = new Properties()
    val src: BufferedSource = Source.fromFile(file)
    props.load(src.bufferedReader())
    src.close()
    props
  }
}
