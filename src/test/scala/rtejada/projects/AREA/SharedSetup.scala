package rtejada.projects.AREA

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.apache.spark.SparkContext
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.log4j.PropertyConfigurator

/** Shared setup for all tests, both before and after they are run */
trait SharedSetup extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient private var _sc: SparkContext = _
  private var spark: SparkSession = _
  var testData: DataFrame = _
  var testConfig: DataFrame = _

  override val conf = new SparkConf().setAppName("AREA").setMaster("local[4]")
    .set("spark.ui.showConsoleProgress", "false").
    set("spark.ui.enabled", "false")

  // Logging configuration
  PropertyConfigurator.configure("log4j.properties")

  override def sc: SparkContext = _sc

  /** Will run before any tests are run. */
  override def beforeAll() {
    _sc = new SparkContext(conf)
    spark = SparkSession.builder().appName("AREA").getOrCreate()

    testData = spark.read.option("header", false).csv("data/runwayFlights_PHX_*.csv")
    testConfig = spark.read.option("header", true).csv("data/exit_config_PHX.csv")

    super.beforeAll()
  }

  /** Will run after all tests have been run. */
  override def afterAll() {
    try {
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }
}