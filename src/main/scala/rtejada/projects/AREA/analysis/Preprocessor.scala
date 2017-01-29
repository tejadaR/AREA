/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA.analysis

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat
import scala.util.control.Breaks._
import rtejada.projects.AREA.Main.spark.implicits._

/** Filters bad data, extracts features and ensures dataset is labaled */
class Preprocessor(inputData: DataFrame, inputConfig: DataFrame, airportCode: String) extends Serializable {
  //Transforming exit configuration DF to Array[Row]
  val exitConfig = inputConfig.collect()

  //Duplicate-name columns to drop
  val withoutDupDF = inputData.drop("_c13", "_c16")

  //Rename Columns
  val headerSeq = withoutDupDF.head().toSeq.asInstanceOf[Seq[String]]
  val inputDF = withoutDupDF.toDF(headerSeq: _*)

  //Filter
  val filteredDF = filterFlights(inputDF, airportCode)

  //Adding all features
  val fullFeaturesDF = addFeatures(filteredDF)

  val readyDF = findExit(fullFeaturesDF, exitConfig)

  val finalDF = readyDF.select(
    "runway",
    "depAirport",
    "aircraftType",
    "arrTerminal",
    //"arrGate",
    "touchdownLat",
    "touchdownLong",
    "hour",
    //"day",
    "speed1",
    "speed2",
    //"carrier",
    "traffic",
    "exit")

  /** Remove irrelevant Columns, null values and departure records. */
  private def filterFlights(inputDF: DataFrame, airport: String): DataFrame = {
    //Dropping irrelevant columns and filtering departures out
    val relevantDF = inputDF.filter(s"arrAirport=='$airport'").select("callsign", "runway", "positions",
      "depAirport", "arrAirport", "aircraftType", "arrTerminal", "arrGate")

    //Dropping null values
    val filteredDF = relevantDF.filter(relevantDF.columns.map(c => col(c) =!= "null").reduce(_ and _))
    filteredDF
  }

  /** Extract necessary features and add as columns. */
  private def addFeatures(inputDF: DataFrame): DataFrame = {
    val withTouchdown = findTouchdownPos(inputDF)
    val withDayTime = findDayTime(withTouchdown)
    val withSpeeds = findSpeeds(withDayTime)
    val withCarrier = findCarrier(withSpeeds)
    val withTraffic = findTraffic(findLandingTime(withCarrier))
    withTraffic
  }

  /**
   * Finds the carrier using letters in the callsign column
   */
  private def findCarrier(inputDF: DataFrame): DataFrame = {
    val toStringUDF = udf((a: Seq[String]) => a.mkString(""))
    inputDF.withColumn("carrier", toStringUDF(split(inputDF.col("callsign"), "\\d")))
  }

  /**
   * Finds exit using a user-defined function.
   *
   * It iterates over positions to find the first
   * one within an exit's lat & long thresholds
   *
   * Returns DataFrame labeled with an exit column
   */
  private def findExit(inputDF: DataFrame, exitCfg: Array[Row]): DataFrame = {

    def calcExit(positions: String) = {
      val touchDownLat = Math.abs(positions.split("\\|")(0).split(";")(0).toDouble)
      val threshold = 0.00096
      val ifExit = (exitCfg: Array[Row], inStr: String) => {
        val thresholdLat = 0.00016
        val latInput = Math.abs(inStr.split(";")(0).toDouble)
        val longInput = Math.abs(inStr.split(";")(1).toDouble)
        val isExit = (row: Row) => {
          val thisLat = row.apply(1).asInstanceOf[String].toDouble
          val thisLongUpper = row.apply(2).asInstanceOf[String].toDouble
          val thisLongLower = row.apply(3).asInstanceOf[String].toDouble

          val latCondition = Math.abs(latInput - thisLat) < thresholdLat
          val longCondition = longInput < thisLongUpper && longInput > thisLongLower
          val farCondition = (latInput - touchDownLat) < threshold
          if (latCondition && longCondition && farCondition) true else false
        }
        exitCfg.find(isExit(_)) match {
          case Some(row: Row) => row.apply(0).asInstanceOf[String]
          case None           => "none"
        }
      }
      positions.split("\\|").drop(1).find(pos => !ifExit(exitCfg, pos).matches("none")) match {
        case Some(s) => ifExit(exitCfg, s)
        case None    => "none"
      }
    }
    val exitUDF = udf(calcExit(_: String))
    val exitCol = exitUDF.apply(inputDF.col("positions"))
    val withExitDF = inputDF.withColumn("exit", exitCol).filter("exit!='none'")

    withExitDF
  }

  /**
   * Finds Touchdown Position using a user-defined function.
   *
   * It retrieves the first position and
   * considers it the Touchdown position.
   *
   * Returns DataFrame with touchdownPosition
   */
  private def findTouchdownPos(inputDF: DataFrame): DataFrame = {
    //latOrLong = 0 for latitude, latOrLong = 1 for longitude
    def calcTouchdownPos(positions: String, latOrLong: Integer): Double = {
      val splitPositions = positions.split("\\|")
      val splitFirstPos = splitPositions(0).split(";")
      val coords = splitFirstPos(latOrLong).toDouble
      Math.abs(coords)
    }

    val latitudeUDF = udf(calcTouchdownPos(_: String, 0))
    val longitudeUDF = udf(calcTouchdownPos(_: String, 1))

    val latitudeCol = latitudeUDF.apply(inputDF.col("positions"))
    val longitudeCol = longitudeUDF.apply(inputDF.col("positions"))

    val withTouchdownDF = inputDF.withColumn("touchdownLat", latitudeCol.cast(DoubleType)).withColumn("touchdownLong", longitudeCol.cast(DoubleType))

    withTouchdownDF
  }

  /**
   * Finds Day and Time using a user-defined function.
   *
   * It gets epoch time on first link and formats it.
   *
   * timeFormat = "HH" for hour of day,
   * timeFormat = "e" for day of week
   *
   * Returns DataFrame with date and time columns.
   */
  private def findDayTime(inputDF: DataFrame): DataFrame = {

    def calcDayTime(positions: String, format: String): String = {
      val splitFirstLink = positions.split("\\|")(0).split(";")
      val epochSplit = splitFirstLink(2).split("E")
      val decimal = epochSplit(0).toDouble
      val exp = epochSplit(1).toDouble
      val milliseconds: Long = (decimal * Math.pow(10, exp)).toLong

      def getTimeDecimal(epochMillis: Long): String =
        DateTimeFormat.forPattern(format).print(epochMillis)

      getTimeDecimal(milliseconds)
    }
    val hourUDF = udf(calcDayTime(_: String, "HH"))
    val hourCol = hourUDF.apply(inputDF.col("positions"))
    val withHourDF = inputDF.withColumn("hour", hourCol)

    val dayUDF = udf(calcDayTime(_: String, "e"))
    val dayCol = dayUDF.apply(inputDF.col("positions"))
    val withDayDF = withHourDF.withColumn("day", dayCol)

    withDayDF
  }

  /**
   * Finds breaking speeds using a user-defined function.
   *
   * Uses longitudes and epochs on a set of recorded positions to find an
   * average landing speed. If there are less than 20 recorded positions,
   * it will take the average of any available positions
   *
   * Returns DataFrame with two breaking speed columns
   */
  private def findSpeeds(inputDF: DataFrame): DataFrame = {

    def calcSpeed(positions: String, set: Integer): Double = {
      val posArraySize = positions.split("\\|").length
      val isEven = (number: Int) => number % 2 == 0

      val positionsUsed = if (posArraySize < 20) {
        if (isEven(posArraySize)) posArraySize / 2 else (posArraySize - 1) / 2
      } else 10

      def getMilliseconds(input: Array[String]): Long = {
        val decimal = input(2).split("E")(0).toDouble
        val exponential = input(2).split("E")(1).toDouble
        (decimal * Math.pow(10, exponential)).toLong
      }
      val posUsedArr = positions.split("\\|").slice((set * positionsUsed),
        (set * positionsUsed) + positionsUsed)
      val speedArr = posUsedArr.sliding(2).map(pos => {
        val curMilliseconds = getMilliseconds(pos.head.split(";"))
        val curLat = pos.head.split(";")(0)
        val curLong = pos.head.split(";")(1)

        val nextMilliseconds = getMilliseconds(pos.last.split(";"))
        val nextLat = pos.last.split(";")(0)
        val nextLong = pos.last.split(";")(1)

        val diffMilliseconds = Math.abs(curMilliseconds.toDouble - nextMilliseconds.toDouble)
        val diffLat = Math.abs(curLat.toDouble - nextLat.toDouble)
        val diffLong = Math.abs(curLong.toDouble - nextLong.toDouble)
        val euclDist = Math.sqrt(Math.pow(diffLat * 110912.29157913252, 2) +
          Math.pow(diffLong * 92986.56694923184, 2))

        (euclDist / (diffMilliseconds / 1000))
      })
      BigDecimal(speedArr.sum / positionsUsed - 1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    val speedUDF1 = udf(calcSpeed(_: String, 0))
    val speedCol1 = speedUDF1.apply(inputDF.col("positions"))
    val speedUDF2 = udf(calcSpeed(_: String, 1))
    val speedCol2 = speedUDF2.apply(inputDF.col("positions"))

    val withSpeedDF = inputDF.withColumn("speed1", speedCol1.cast(DoubleType)).withColumn("speed2", speedCol2.cast(DoubleType))

    withSpeedDF
  }

  /**
   * Finds the landing time, assumed to be the epoch time on the first recorded
   * position for arrival flights.
   */
  private def findLandingTime(inputDF: DataFrame): DataFrame = {
    def getLandingEpoch(positions: String): Long = {
      val splitFirstLink = positions.split("\\|")(0).split(";")
      val epochSplit = splitFirstLink(2).split("E")
      val decimal = epochSplit(0).toDouble
      val exp = epochSplit(1).toDouble
      (decimal * Math.pow(10, exp)).toLong
    }
    val epochUDF = udf(getLandingEpoch(_: String))
    val epochCol = epochUDF.apply(inputDF.col("positions"))

    inputDF.withColumn("landingEpoch", epochCol)
  }

  /**
   * Finds traffic based on how many additional aircraft landed in a given time interval
   */
  private def findTraffic(inputDF: DataFrame): DataFrame = {
    def getTraffic(thisEpoch: Long, epochsArr: Array[Row]): Long = {
      val uL = thisEpoch + 900000 // + 15 minutes
      val lL = thisEpoch - 900000 // - 15 minutes
      epochsArr.filter(row => {
        row.apply(0).asInstanceOf[Long] > lL &&
          row.apply(0).asInstanceOf[Long] < uL
      }).length.toLong
    }

    val landingEpochsArr = inputDF.select("landingEpoch").collect
    val trafficUDF = udf(getTraffic(_: Long, landingEpochsArr))
    val trafficCol = trafficUDF.apply(inputDF.col("landingEpoch"))

    inputDF.withColumn("traffic", trafficCol)
  }

}