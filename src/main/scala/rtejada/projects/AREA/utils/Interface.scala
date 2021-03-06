/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.utils

import org.apache.spark.sql._
import java.util.Calendar
import java.io.PrintWriter
import rtejada.projects.AREA.Main
import java.io.File
import scala.io.StdIn.readLine
import scala.util.Try
import org.apache.spark.ml.PipelineModel

object Interface {

  /** Input prompt, returns entered String. */
  def inputPrompt(message: String) = readLine(message)

  /** Outputs string to given fileName. */
  def output(out: String, fileName: String): Unit = output(out, fileName, true)

  /** Outputs string to given fileName. Appending a header with a datetime is optional */
  def output(out: String, fileName: String, withoutDate: Boolean) = {
    val file = new File("output/" + fileName)
    if (!file.getParentFile.exists) file.getParentFile.mkdir
    val pw = new PrintWriter(file)

    val header = (System.lineSeparator() +
      "----------------------------------------------" +
      System.lineSeparator() +
      Calendar.getInstance().getTime +
      System.lineSeparator())

    if (withoutDate) pw.write(out) else pw.write(header + out)

    pw.close()
  }

  /** Saves model to a local folder*/
  def saveModel(model: PipelineModel, modelID: Long, airport: String) = {
    val file = new File("trained")
    if (!file.exists) file.mkdir
    model.save("trained/" + airport + "model" + modelID)
  }

  /** Outputs JSON with a datetime value to given fileName. */
  def outputJsonWithDate(info: Map[String, String], fileName: String) = {
    val file = new File("output/" + fileName)
    if (!file.getParentFile.exists) file.getParentFile.mkdir
    val pw = new PrintWriter(file)

    def assemble(value: (String, String)): String = {
      def isAllDigits(x: String) = x forall Character.isDigit

      if (value._2.split('.').length < 3 &&
        isAllDigits(value._2.split('.').head) &&
        isAllDigits(value._2.split('.').last)) //Numerical
        "\"" + value._1 + "\":" + value._2 + ",\n"
      else // String
        "\"" + value._1 + "\":\"" + value._2 + "\",\n"

    }

    pw.write("{" + info.map(assemble(_)).mkString +
      "\"date\":\"" + Calendar.getInstance().getTime + "\"\n" + "}")
    pw.close()
  }

  /**
   * Prints DataFrame to given fileName.
   * DO NOT USE WITH HDFS OR IN PRODUCTION
   */
  def printDFtoFile(inputDF: DataFrame, fileName: String) {
    inputDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(fileName);
  }

  /** Returns Tuple2 with process and a String with time elapsed. */
  def getProcessDuration[T](proc: => T): Tuple2[T, String] = {
    val start = System.nanoTime()
    val process = proc
    val end = System.nanoTime()
    val timeStr = "Task completed, time elapsed: " + (end - start) / 1000000000 + " seconds"
    (process, timeStr)
  }

  /** Loading method for reading data csv files into spark */
  def getAirportData(airport: String, spark: SparkSession): DataFrame = airport match {
    case ic"PHX" | ic"PHOENIX"   => spark.read.option("header", false).csv("data/runwayFlights_PHX_*.csv")
    case ic"ATL" | ic"ATLANTA"   => spark.read.option("header", false).csv("data/runwayFlights_ATL_*.csv")
    case ic"BWI" | ic"BALTIMORE" => spark.read.option("header", false).csv("data/runwayFlights_BWI_*.csv")
    case ic"DEN" | ic"DENVER"    => spark.read.option("header", false).csv("data/runwayFlights_DEN_*.csv")
  }

  /** Loading method for reading data csv files into spark */
  def getLinks(airport: String, spark: SparkSession): (DataFrame, DataFrame) = airport match {
    case "KPHX" => (spark.read.option("header", true).csv("testfiles/PHX-Nodes.csv"),
      spark.read.option("header", true).csv("testfiles/PHX-Links.csv"))
    case "KATL" => (spark.read.option("header", true).csv("testfiles/ATL-Nodes.csv"),
      spark.read.option("header", true).csv("testfiles/ATL-Links.csv"))
    case "KBWI" => (spark.read.option("header", true).csv("testfiles/BWI-Nodes.csv"),
      spark.read.option("header", true).csv("testfiles/BWI-Links.csv"))
    case "KDEN" => (spark.read.option("header", true).csv("testfiles/DEN-Nodes.csv"),
      spark.read.option("header", true).csv("testfiles/DEN-Links.csv"))
  }

  def getSizeDefinition(spark: SparkSession) = spark.read.option("header", true).csv("data/sizedef.csv")

  /** Loading method for reading config csv files into spark */
  def getExitConfig(airport: String, spark: SparkSession): DataFrame = airport match {
    case ic"PHX" | ic"PHOENIX"   => spark.read.option("header", true).csv("data/exit_config_PHX.csv")
    case ic"ATL" | ic"ATLANTA"   => spark.read.option("header", true).csv("data/exit_config_ATL.csv")
    case ic"BWI" | ic"BALTIMORE" => spark.read.option("header", true).csv("data/exit_config_BWI.csv")
    case ic"DEN" | ic"DENVER"    => spark.read.option("header", true).csv("data/exit_config_DEN.csv")
  }

  /** Formats airport code*/
  def getAirportCode(airport: String): String = airport match {
    case ic"PHX" | ic"PHOENIX"   => "KPHX"
    case ic"ATL" | ic"ATLANTA"   => "KATL"
    case ic"BWI" | ic"BALTIMORE" => "KBWI"
    case ic"DEN" | ic"DENVER"    => "KDEN"
  }

  private implicit class IgnoreCaseRegex(sc: StringContext) {
    def ic = ("(?i)" + sc.parts.mkString).r
  }

}