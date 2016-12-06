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
  val airportName = Interface.inputAirport("Enter Airport: ")

  try {
    val dataDF = Interface.getAirportData(airportName).cache
    val configDF = Interface.getExitConfig(airportName)

    val analysis = new ExitAnalysis(dataDF, configDF)
    analysis.processedDF.show

    val predictions = analysis.forestHandler.predictions
    val accuracy = analysis.forestHandler.accuracy

    predictions.show
    println(f"RandomForestClassifier Model Accuracy: $accuracy%2.2f%% using ${predictions.count} test records")

  } catch {
    case ex: FileNotFoundException => println(s"Data or config not found for Airport: \'$airportName\' " + ex)
    case ex: AnalysisException     => println(s"Invalid query using Airport: \'$airportName\' " + ex)
    case ex: MatchError            => println("Unable to match: " + ex.getMessage())
    case ex: IOException           => println("IO Exception " + ex)
    case other: Throwable          => println("Exception: " + other)

  } finally {
    println("Exiting")
  }

}