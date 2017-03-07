/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

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

  try {
    val mainApp = new MainApp().main(args)
  } catch {
    case ex: FileNotFoundException => println("Data or config data not found " + ex)
    case ex: AnalysisException     => println("Invalid query using selected airport" + ex)
    case ex: MatchError            => println("Unable to match: " + ex.getMessage())
    case ex: IOException           => println("IO Exception " + ex)
    case other: Throwable          => println("Exception: " + other.printStackTrace())

  } finally {
    println("Exiting " + Calendar.getInstance().getTime)
  }

}