package datavalidation.spark.utils

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{Logging, SparkConf}
import org.scalatest.Suite

trait SparkMocker extends Logging {
  this: Suite =>

  private val tempDir: File = MockerUtils.createTempDir()
  private val localWarehousePath: File = new File(tempDir, "warehouse")
  private val localMetastorePath = new File(tempDir, "metastore")
  var appLogLevel: Level = Level.INFO

  def getSparkSession: SparkSession = {
    val ssb: SparkSession.Builder = SparkSession.builder.config(sparkConfig())
    if (enableHiveSupportFlag) ssb.enableHiveSupport()
    val ss: SparkSession = ssb.getOrCreate()
    ss.sparkContext.setLogLevel(appLogLevel.toString)
    ss
  }

  def enableHiveSupportFlag: Boolean = true

  def sparkConfig(sparkConf: SparkConf = null): SparkConf = {
    if (sparkConf == null) {
      val conf: SparkConf = new SparkConf()
      conf.setAppName("DataValidation_CucumberTesting")
      conf.set("spark.app.id", applicationID)
      conf.setMaster("local[2]")
      conf.set("spark.ui.enabled", "false")
      conf.set(SQLConf.CODEGEN_FALLBACK.key, "false")
      conf.set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${localMetastorePath.getCanonicalPath};create=true")
      conf.set("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      conf.set(ConfVars.METASTOREURIS.varname, "")
      conf.set("spark.sql.streaming.checkpointLocation", tempDir.toPath.toString)
      conf.set("spark.sql.warehouse.dir", localWarehousePath.getCanonicalPath)
      conf.set("spark.sql.shuffle.partitions", "1")
      conf.set("spark.launcher.childProcLoggerName", "datavalidationlogger")
      conf.set("spark.sql.parquet.compression.codec", "snappy")
      conf.set("hive.exec.dynamic.partition", "true")
      conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      conf
    } else {
      sparkConf
    }
  }

  def applicationID: String = this.getClass.getName + math.floor(math.random * 10E4).toLong.toString

  def stopSparkSession(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  }

  case class WrappedConfVar(cv: ConfVars) {
    val varname: String = cv.varname

    def getDefaultExpr: String = cv.getDefaultExpr
  }

}