/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA.utils

import org.apache.spark.sql._
import java.util.Calendar
import java.io.PrintWriter
import rtejada.projects.AREA.Main
import java.io.File
import scala.io.StdIn.readLine
import scala.util.Try

object Interface {

  /** Input prompt, returns entered airport name. */
  def inputAirport(message: String) = readLine(message)

  /** Outputs string to given fileName. */
  def output(out: String, fileName: String): Unit = output(out, fileName, true)

  /** Outputs string to given fileName. */
  def output(out: String, fileName: String, withoutDate: Boolean): Unit = {
    val pw = new PrintWriter(new File("output/" + fileName))

    val header = (System.lineSeparator() +
      "----------------------------------------------" +
      System.lineSeparator() +
      Calendar.getInstance().getTime +
      System.lineSeparator())

    if (withoutDate) pw.write(out) else pw.write(header + out)

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

  def getAirportData(airport: String): DataFrame = airport match {
    case ic"PHX" | ic"PHOENIX" => Main.spark.read.option("header", false).csv("data/runwayFlights_PHX_*.csv")
    case ic"ATL" | ic"ATLANTA" => Main.spark.read.option("header", false).csv("data/runwayFlights_ATL_*.csv")
    case ic"DNV" | ic"ATLANTA" => Main.spark.read.option("header", false).csv("data/runwayFlights_DNV_*.csv")
  }

  def getExitConfig(airport: String): DataFrame = airport match {
    case ic"PHX" | ic"PHOENIX" => Main.spark.read.option("header", true).csv("data/exit_config_PHX.csv")
    case ic"ATL" | ic"ATLANTA" => Main.spark.read.option("header", true).csv("data/exit_config_ATL.csv")
    case ic"DNV" | ic"ATLANTA" => Main.spark.read.option("header", true).csv("data/exit_config_DNV.csv")
  }

  private implicit class IgnoreCaseRegex(sc: StringContext) {
    def ic = ("(?i)" + sc.parts.mkString).r
  }

}