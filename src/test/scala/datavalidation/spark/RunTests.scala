package datavalidation.spark

import datavalidation.spark.tests.BasicValidationTest
import org.scalatest.Suites

class RunTests extends Suites(
  new BasicValidationTest
)
