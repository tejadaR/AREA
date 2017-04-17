/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

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
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.graphframes.GraphFrame

/** Model implementation */
class AREAModel {

  val airportSeq = Seq("PHX", "ATL", "BWI")
  val treeSeq = Seq(40, 85, 115, 165)
  val depthSeq = Seq(5, 6, 7, 8)
  var optLoadedModel: Option[(String, PipelineModel, ForestRun)] = None

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

  def loadLinks(airportCode: String) = {
    val dataFrames = Interface.getLinks(airportCode, spark)
    val vertexDF = dataFrames._1.select("nodeName", "latitude", "longitude").withColumnRenamed("nodeName", "id")
    val edgeDF = dataFrames._2.select("LinkID", "LinkName", "NodeNameFrom", "NodeNameTo").
      withColumnRenamed("NodeNameFrom", "src").withColumnRenamed("NodeNameTo", "dst")
    val graph = GraphFrame(vertexDF, edgeDF)
    graph.vertices.createOrReplaceTempView("Vertices")
    graph.edges.createOrReplaceTempView("Edges")
    val withSrcCoords = spark.sql("SELECT LinkID, LinkName, src, dst," +
      "latitude AS srcLatitude, longitude AS srcLongitude " +
      "FROM Edges INNER JOIN Vertices ON src = id")
    withSrcCoords.createOrReplaceTempView("Edges")
    val withSrcAndDst = spark.sql("SELECT LinkID, LinkName, src, dst, srcLatitude, srcLongitude," +
      "latitude AS dstLatitude, longitude AS dstLongitude " +
      "FROM Edges INNER JOIN Vertices ON dst = id")
    (withSrcAndDst, graph)
  }

  /** Creates a single-row DF to test the currently loaded model */
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
    optLoadedModel match {
      case Some(model) => {
        val prediction = model._2.transform(testDF)
        prediction.select("predictedExit").head.toString
      }
      case _ => "No model loaded"
    }
  }

  /** Loads a new model into the modelMap HashMap */
  def loadModel(fullModelID: String, view: OptionsView) = {
    val id = fullModelID.filter(_.isDigit)
    val forestRun = new ForestRun("output/features" + id + ".json",
      "output/randomForest" + id + ".txt",
      "output/runInfo" + id + ".json",
      "output/optimization" + id + ".json")
    optLoadedModel = Some(fullModelID, PipelineModel.load("trained/" + fullModelID), forestRun)
    forestRun
  }

  /** Gets the trained model names in file*/
  def getModels: Seq[String] = {
    val dir = new File("trained")
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isDirectory).toSeq.map(_.getName) else Seq[File]().map(_.getName)
  }

  /** Starts the pre-processing and machine-learning pipeline tasks for the parameters selected.*/
  def runModel(airport: String, treeNum: Int, depthNum: Int, featureList: List[String], view: OptionsView): String = {
    val startEpoch = Calendar.getInstance.getTimeInMillis

    val dataDF = Interface.getAirportData(airport, spark)
    val configDF = Interface.getExitConfig(airport, spark)
    val airportCode = Interface.getAirportCode(airport)
    Platform.runLater {
      view.analysisBox.statusLabel.text = "Pre-processing data"
      view.analysisBox.runPb.progress = 0.1
    }
    val rawVertEdgeDF = Interface.getLinks(airportCode, spark)

    val preProcessor = new Preprocessor(spark, dataDF, configDF, airportCode, featureList, rawVertEdgeDF)
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

    val sizeDefDF = Interface.getSizeDefinition(spark)
    val optimization = new Optimization(spark, preProcessor, predictions,
      preProcessor.verticesDF, preProcessor.exitEdgesDF, sizeDefDF, forestHandler.runTimeId)

    val runDuration = Calendar.getInstance.getTimeInMillis - startEpoch

    val runInfoMap: Map[String, String] = Map("airportCode" -> airportCode,
      "accuracy" -> accuracy.toString,
      "numRunways" -> preProcessor.finalDF.select("runway").distinct.count.toString,
      "numExits" -> preProcessor.finalDF.select("exit").distinct.count.toString,
      "trainCount" -> trainCount.toString,
      "testCount" -> testCount.toString,
      "runDuration" -> runDuration.toString)

    Interface.saveModel(forestHandler.finalModel, forestHandler.runTimeId, airportCode)
    Interface.outputJsonWithDate(runInfoMap, "runInfo" + forestHandler.runTimeId + ".json")
    forestHandler.runTimeId.toString
  }

}