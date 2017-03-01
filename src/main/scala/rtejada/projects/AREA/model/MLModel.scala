package rtejada.projects.AREA.model

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util._
import java.io.FileNotFoundException
import java.io.IOException
import javax.script.ScriptException
import java.util.Calendar
import rtejada.projects.AREA.utils.Interface
import rtejada.projects.AREA.controller.OptionsController
import rtejada.projects.AREA.view.OptionsView
import scalafx.application.Platform
import java.io.File
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import org.apache.spark.sql.types.StructType

/** Model implementation */
class MLModel {

  val airportSeq = Seq("PHX", "ATL", "BWI", "DEN")
  val treeSeq = Seq(60, 85, 115)
  val depthSeq = Seq(5, 6, 7)
  val modelMap = HashMap[String, Tuple2[PipelineModel, Array[Feature]]]()

  PropertyConfigurator.configure("log4j.properties") // Logging configuration
  val conf = new SparkConf().setAppName("AREA").setMaster("local[*]")
    .set("spark.executor.memory", "4g")
    .set("spark.executor.driver", "4g")
    .set("spark.driver.memory", "5g")
    .set("spark.ui.showConsoleProgress", "false")
  System.setProperty("hadoop.home.dir", "c:\\winutil\\") //running on windows
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("AREA")
    .getOrCreate()
  import spark.implicits._

  def testSingleRecord(paramArr: Array[(String, String, String)]): String = {
    val values = Seq(paramArr.map(_._3).mkString(","))
    val singleDF = values.toDF
    val paramDfArr = paramArr.map(entry => {
      if (entry._2 == "categorical")
        Seq(entry._3).toDF(entry._1)
      else
        Seq(entry._3.toDouble).toDF(entry._1)
    })
    val testDF = paramDfArr.reduceLeft((a, b) => { a.withColumn(b.columns.head, lit(b.head.apply(0))) })
    val prediction = modelMap.head._2._1.transform(testDF)
    prediction.select("predictedExit").head.toString
  }

  def loadModel(modelName: String, view: OptionsView) = {
    val id = modelName.filterNot("model".toSet)
    if (modelMap.size > 0) modelMap.clear()
    modelMap += (id -> (PipelineModel.load("trained/" + modelName), new ForestRun("output/features" + id + ".json",
      "output/randomForest" + id + ".txt",
      "output/runInfo" + id + ".json").featureExtracted.dropRight(1)))
    modelMap.apply(id)._2
  }

  def getModels: Seq[String] = {
    val dir = new File("trained")
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isDirectory).toSeq.map(_.getName) else Seq[File]().map(_.getName)
  }

  def runModel(airport: String, treeNum: Int, depthNum: Int, view: OptionsView): String = {
    val startEpoch = Calendar.getInstance.getTimeInMillis

    val dataDF = Interface.getAirportData(airport, spark)
    val configDF = Interface.getExitConfig(airport, spark)
    val airportCode = Interface.getAirportCode(airport, spark)
    Platform.runLater {
      view.analysisBox.statusLabel.text = "Pre-processing data"
      view.analysisBox.runPb.progress = 0.1
    }
    val preProcessor = new Preprocessor(dataDF, configDF, airportCode)
    val processedDF = preProcessor.finalDF.cache()
    Platform.runLater {
      view.analysisBox.statusLabel.text = "Generating features..."
      view.analysisBox.runPb.progress = 0.3
    }
    val forestHandler = new ForestHandler(processedDF, view, treeNum, depthNum)
    val predictions = forestHandler.predictions
    val accuracy = forestHandler.accuracy
    val trainCount = forestHandler.trainingData.count
    val testCount = forestHandler.testingData.count
    val runDuration = Calendar.getInstance.getTimeInMillis - startEpoch

    predictions.show(false)

    println(f"RandomForestClassifier Model Accuracy: $accuracy%2.2f%% using ${testCount} test records")
    println(forestHandler.bestParams)

    val runInfoMap: Map[String, String] = Map("airportCode" -> airportCode,
      "accuracy" -> accuracy.toString,
      "numRunways" -> preProcessor.finalDF.select("runway").distinct.count.toString,
      "numExits" -> preProcessor.finalDF.select("exit").distinct.count.toString,
      "trainCount" -> trainCount.toString,
      "testCount" -> testCount.toString,
      "runDuration" -> runDuration.toString)

    Interface.saveModel(forestHandler.finalModel, forestHandler.runTimeId)
    Interface.outputJsonWithDate(runInfoMap, "runInfo" + forestHandler.runTimeId + ".json")
    f"RandomForestClassifier Model Accuracy: $accuracy%2.2f%% using ${testCount} test records"
  }

 

}