package rtejada.projects.AREA.analysis

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import rtejada.projects.AREA.SharedSetup

class PreprocessorSpec extends FlatSpec with SharedSetup {

  "A preprocessor" should "filter out departures" in {
    val pp = new Preprocessor(testData, testConfig)
    val departureFound = pp.filteredDF.select("depAirport").collect().find(_.apply(0).toString.equals("KPHX"))
    assert(departureFound == None)
  }

  "A preprocessor" should "extract required features" in {
    val pp = new Preprocessor(testData, testConfig)
    assert(pp.fullFeaturesDF.columns.contains("touchdownLong"))
    assert(pp.fullFeaturesDF.columns.contains("hour"))
    assert(pp.fullFeaturesDF.columns.contains("day"))
    assert(pp.fullFeaturesDF.columns.contains("speed1"))
    assert(pp.fullFeaturesDF.columns.contains("speed2"))
  }
  
  "A preprocessor" should "produce labeled data (with exit column)" in {
    val pp = new Preprocessor(testData, testConfig)
    assert(pp.finalDF.columns.contains("exit"))
  }
  
  "A preprocessor's output" should "not have any nulls" in {
    val pp = new Preprocessor(testData, testConfig)
    val nullFound = pp.finalDF.collect().find(_.anyNull)

    assert(nullFound == None)
  }

}