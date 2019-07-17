package datavalidation.spark.dto

import org.apache.spark.sql.types.StructType

case class SchemaDTO(schema: List[Field]) {

  def toStructType: StructType = {
    StructType.fromDDL(schema.map(x => s"${x.fieldName} ${x.fieldType}").mkString(","))
  }
}
