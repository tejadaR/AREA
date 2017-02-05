/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA

import org.apache.log4j.PropertyConfigurator
import rtejada.projects.AREA.utils.Interface
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.App
import scala.util._
import java.io.FileNotFoundException
import java.io.IOException
import javax.script.ScriptException
import java.util.Calendar

/** Entry point for project AREA.*/
object Main extends App {
  //Initialization
  PropertyConfigurator.configure("log4j.properties") // Logging configuration
  val conf = new SparkConf().setAppName("AREA").setMaster("local[4]").set("spark.executor.memory", "12g")
    .set("spark.ui.showConsoleProgress", "false")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("AREA")
    .getOrCreate()
  //Input
  val airportName = Interface.inputPrompt("Enter Airport: ")

  try {
    val startEpoch = Calendar.getInstance.getTimeInMillis
    
    val dataDF = Interface.getAirportData(airportName).cache
    val configDF = Interface.getExitConfig(airportName)
    val airportCode = Interface.getAirportCode(airportName)

    val analysis = new ExitAnalysis(dataDF, configDF, airportCode)
    analysis.processedDF.show

    val predictions = analysis.forestHandler.predictions
    val accuracy = analysis.forestHandler.accuracy
    
    val trainCount = analysis.forestHandler.trainingData.count
    val testCount = analysis.forestHandler.testingData.count    

    predictions.show(false)
    
    println(f"RandomForestClassifier Model Accuracy: $accuracy%2.2f%% using ${testCount} test records")
    println(analysis.forestHandler.bestParams)

    val runDuration = Calendar.getInstance.getTimeInMillis - startEpoch
    
    val runInfoMap: Map[String, String] = Map("airportCode" -> airportCode,
      "accuracy" -> accuracy.toString,
      "numRunways" -> analysis.preProcessor.finalDF.select("runway").distinct.count.toString,
      "numExits" -> analysis.preProcessor.finalDF.select("exit").distinct.count.toString,
      "trainCount" -> trainCount.toString,
      "testCount" -> testCount.toString,
      "runDuration" -> runDuration.toString)

    Interface.outputJsonWithDate(runInfoMap, "runInfo"+analysis.forestHandler.runTimeId+".json")
    
  } catch {
    case ex: FileNotFoundException => println(s"Data or config not found for Airport: \'$airportName\' " + ex)
    case ex: AnalysisException     => println(s"Invalid query using Airport: \'$airportName\' " + ex)
    case ex: MatchError            => println("Unable to match: " + ex.getMessage())
    case ex: IOException           => println("IO Exception " + ex)
    case other: Throwable          => println("Exception: " + other.printStackTrace())

  } finally {
    println("Exiting " + Calendar.getInstance().getTime)
  }

}