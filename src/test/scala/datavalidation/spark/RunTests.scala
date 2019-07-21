package datavalidation.spark

import datavalidation.spark.tests.{BasicValidationTest, FileComparisonTest}
import org.scalatest.Suites

class RunTests extends Suites(
  new BasicValidationTest,
  new FileComparisonTest
)
