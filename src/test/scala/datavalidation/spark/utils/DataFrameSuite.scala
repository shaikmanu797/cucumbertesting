package datavalidation.spark.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

trait DataFrameSuite extends FunSuite with SparkMocker with DataFrameSuiteBase with BeforeAndAfterAll with BeforeAndAfter {

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = getSparkSession
    log.info(s"Running tests from test class: ${this.getClass.getName}")
  }

  override def afterAll(): Unit = {
    stopSparkSession(sparkSession)
    log.info(s"Finished tests from test class: ${this.getClass.getName}")
  }
}
