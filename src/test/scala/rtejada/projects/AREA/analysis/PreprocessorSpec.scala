package rtejada.projects.AREA.analysis

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import rtejada.projects.AREA.SharedSetup
import rtejada.projects.AREA.model.Preprocessor

class PreprocessorSpec extends FlatSpec with SharedSetup {
  val testList = List("touchdownLong", "carrier", "hour", "day",
    "decel")

  "A preprocessor" should "filter out departures" in {
    val pp = new Preprocessor(testData, testConfig, "KPHX", testList)
    val departureFound = pp.filteredDF.select("depAirport").collect().find(_.apply(0).toString.equals("KPHX"))
    assert(departureFound == None)
  }

  "A preprocessor" should "extract required features" in {
    val pp = new Preprocessor(testData, testConfig, "KPHX", testList)
    assert(pp.fullFeaturesDF.columns.contains("touchdownLong"))
    assert(pp.fullFeaturesDF.columns.contains("carrier"))
    assert(pp.fullFeaturesDF.columns.contains("hour"))
    assert(pp.fullFeaturesDF.columns.contains("day"))
    assert(pp.fullFeaturesDF.columns.contains("decel"))
  }

  "A preprocessor" should "produce labeled data (with exit column)" in {
    val pp = new Preprocessor(testData, testConfig, "KPHX", testList)
    assert(pp.finalDF.columns.contains("exit"))
  }

  "A preprocessor's output" should "not have any nulls" in {
    val pp = new Preprocessor(testData, testConfig, "KPHX", testList)
    val nullFound = pp.finalDF.collect().find(_.anyNull)

    assert(nullFound == None)
  }

}