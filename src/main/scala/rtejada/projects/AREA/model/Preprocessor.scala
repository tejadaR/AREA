/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat
import scala.util.control.Breaks._
import scala.BigDecimal
import scala.reflect.runtime.universe

/** Filters bad data, extracts features and ensures dataset is labaled */
class Preprocessor(spark: SparkSession, inputData: DataFrame, inputConfig: DataFrame, airportCode: String, featureList: List[String],
                   rawVertEdgeDF: (DataFrame, DataFrame)) extends Serializable {
  import spark.implicits._

  val cleanVertEdgeDF = cleanVE(rawVertEdgeDF)
  val verticesDF = cleanVertEdgeDF._1
  val exitEdgesDF = detectExits(cleanVertEdgeDF._2)

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

  val readyDF = findRealExit(fullFeaturesDF, exitEdgesDF)

  val prelimList = featureList ++ List("positions", "links")
  val finalDF = readyDF.select("exit", prelimList: _*)

  private def cleanVE(input: (DataFrame, DataFrame)): (DataFrame, DataFrame) = {
    val cleanVertices = input._1.select("nodeID", "nodeName", "latitude", "longitude")
    cleanVertices.createOrReplaceTempView("Vertices")
    val initEdges = input._2.select("LinkID", "LinkName", "NodeNameFrom", "NodeNameTo", "Length")
    initEdges.createOrReplaceTempView("Edges")

    val withSrcID = spark.sql("SELECT LinkID, LinkName, NodeNameFrom, NodeNameTo, Length, nodeID AS src " +
      "FROM Edges INNER JOIN Vertices ON NodeNameFrom = nodeName")
    withSrcID.createOrReplaceTempView("Edges")
    val cleanEdges = spark.sql("SELECT LinkID, LinkName, NodeNameFrom, NodeNameTo, Length, src, nodeID AS dst " +
      "FROM Edges INNER JOIN Vertices ON NodeNameTo = nodeName")

    (cleanVertices, cleanEdges)
  }

  private def detectExits(inputDF: DataFrame): DataFrame = {
    val runwayPattern = """[0-9]+\D?\/[0-9]+\D(\D|\.)?\D?\D?[0-9]+?\D?\_[0-9]+\D?\/[0-9]+\D(\D|\.)?\D?\D?[0-9]+?\D?""".r.unanchored
    val exitPattern = """[0-9]+\D?\/[0-9]+\D?(\.|\D)\D?\D?[0-9]+""".r.unanchored
    val isExitUDF = udf((linkName: String) => linkName match {
      case runwayPattern(_*) => 0
      case exitPattern(_*)   => 1
      case _                 => 0
    })
    val exitsDF = inputDF.withColumn("isExit", isExitUDF.apply(inputDF.col("LinkName")).cast(IntegerType))
    exitsDF
  }

  /** Remove irrelevant Columns, null values and departure records. */
  private def filterFlights(inputDF: DataFrame, airport: String): DataFrame = {
    //Dropping irrelevant columns and filtering departures out
    val relevantDF = inputDF.filter(s"arrAirport=='$airport'").select("callsign", "runway", "positions", "links",
      "depAirport", "arrAirport", "aircraftType", "arrGate")

    //Dropping null values
    val filteredDF = relevantDF.filter(relevantDF.columns.map(c => col(c) =!= "null").reduce(_ and _)).
      filter(_.getAs[String]("links").split("\\|").length > 5).
      filter(_.getAs[String]("positions").split("\\|").length > 5)

    val patchDF = if (airport.contains("DEN")) filteredDF.filter(_.getAs[String]("links").matches(";10\\d\\d\\d|"))
    else filteredDF

    patchDF
  }

  /** Extract necessary features and add as columns. */
  private def addFeatures(inputDF: DataFrame): DataFrame = {
    val withTouchdown = findTouchdownPos(inputDF)
    val withDayTime = findDayTime(withTouchdown)
    val withDecel = findDecel(withDayTime)
    val withCarrier = findCarrier(withDecel)
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

  private def findRealExit(inputDF: DataFrame, linkDF: DataFrame): DataFrame = {
    val exitLinkArr = linkDF.filter("isExit=='1'").collect

    def testExit(links: String) = {
      val isExit = (exitLinkArr: Array[Row], link: String) => {
        val linkID = link.split(";")(3)
        exitLinkArr.find(row => row.getAs[String]("LinkID") == linkID) match {
          case Some(row: Row) => row.getAs[String]("LinkID")
          case None           => "none"
        }
      }
      links.split("\\|").drop(1).find(link => !isExit(exitLinkArr, link).matches("none")) match {
        case Some(s) => isExit(exitLinkArr, s)
        case None    => "none"
      }
    }
    val exitUDF = udf(testExit(_: String))
    val exitCol = exitUDF.apply(inputDF.col("links"))
    val withExitDF = inputDF.withColumn("exit", exitCol).filter("exit!='none'")

    withExitDF
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

    def getAngle(long1: Double, lat1: Double, long2: Double, lat2: Double) = {
      val longDiff = Math.abs(Math.abs(long1) - Math.abs(long2))
      val latDiff = Math.abs(lat1 - lat2)
      if (longDiff > 0 || latDiff > 0) Math.toDegrees(Math.atan(latDiff / longDiff)) else -999
    }

    def calcExit(positions: String) = {
      val touchDownLat = Math.abs(positions.split("\\|")(0).split(";")(0).toDouble)
      val touchDownLong = Math.abs(positions.split("\\|")(0).split(";")(1).toDouble)
      val nextIndex = if (positions.split("\\|").length > 3) 3 else positions.split("\\|").length - 1
      val nextLat = Math.abs(positions.split("\\|")(nextIndex).split(";")(0).toDouble)
      val nextLong = Math.abs(positions.split("\\|")(nextIndex).split(";")(1).toDouble)

      val runwayAngle = getAngle(touchDownLong, touchDownLat, nextLong, nextLat)

      val angleThreshold = 4 //degrees(angle)
      val ifExit = (exitCfg: Array[Row], inStr: String) => {
        val thresholdLat = 0.00016
        val latInput = Math.abs(inStr.split(";")(0).toDouble)
        val longInput = Math.abs(inStr.split(";")(1).toDouble)
        val isExit = (row: Row) => {
          val thisLat = row.apply(1).asInstanceOf[String].toDouble
          val thisLongUpper = row.apply(2).asInstanceOf[String].toDouble
          val thisLongLower = row.apply(3).asInstanceOf[String].toDouble

          val latCondition = Math.abs(latInput - thisLat) < thresholdLat
          val longCondition = Math.abs(longInput) < Math.abs(thisLongUpper) &&
            Math.abs(longInput) > Math.abs(thisLongLower)
          val thisAngle = getAngle(touchDownLong, touchDownLat, longInput, latInput)
          val farExitCondition = Math.abs(runwayAngle - thisAngle) < angleThreshold
          if (latCondition && longCondition && farExitCondition) true else false
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
      val positionsArr = positions.split("\\|")
      val firstPosArr = positionsArr(0).split(";")
      val coords = firstPosArr(latOrLong).toDouble
      coords
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

    val dayUDF = udf(calcDayTime(_: String, "E"))
    val dayCol = dayUDF.apply(inputDF.col("positions"))
    val withDayDF = withHourDF.withColumn("day", dayCol)

    withDayDF
  }

  /**
   * Finds deceleration using a user-defined function.
   *
   * Uses the first 20 recorded positions to calculate deceleration.
   * If there are less than 20 recorded positions,
   * it will use any available positions
   *
   * Returns DataFrame with a "decel"(deceleration) columns
   */
  private def findDecel(inputDF: DataFrame): DataFrame = {

    def calcSpeed(positions: String, set: Integer): Double = {
      val posArraySize = positions.split("\\|").length
      val positionsUsed = Math.min(posArraySize, 20)

      def getMilliseconds(latLongEpochArr: Array[String]): Long = {
        val decimal = latLongEpochArr(2).split("E")(0).toDouble
        val exponential = latLongEpochArr(2).split("E")(1).toDouble
        (decimal * Math.pow(10, exponential)).toLong
      }
      val posUsedArr = positions.split("\\|").slice(0, positionsUsed)

      val firstLat = posUsedArr.head.split(";")(0).toDouble
      val firstLong = posUsedArr.head.split(";")(1).toDouble
      val firstMilliseconds = getMilliseconds(posUsedArr.head.split(";"))

      val secondLat = posUsedArr(1).split(";")(0).toDouble
      val secondLong = posUsedArr(1).split(";")(1).toDouble
      val secondMilliseconds = getMilliseconds(posUsedArr(1).split(";"))

      val initDistance = getDistance(firstLat, firstLong, secondLat, secondLong)
      val initDiffMilliseconds = Math.abs(firstMilliseconds.toDouble - secondMilliseconds.toDouble)

      val initialSpeed = (initDistance / (initDiffMilliseconds / 1000))

      val penUltLat = posUsedArr.dropRight(1).last.split(";")(0).toDouble
      val penUltLong = posUsedArr.dropRight(1).last.split(";")(1).toDouble
      val penUltMilliseconds = getMilliseconds(posUsedArr.dropRight(1).last.split(";"))

      val lastLat = posUsedArr.last.split(";")(0).toDouble
      val lastLong = posUsedArr.last.split(";")(1).toDouble
      val lastMilliseconds = getMilliseconds(posUsedArr.last.split(";"))

      val endingDistance = getDistance(penUltLat, penUltLong, lastLat, lastLong)
      val endingDiffMilliseconds = Math.abs(penUltMilliseconds.toDouble - lastMilliseconds.toDouble)

      val endingSpeed = (endingDistance / (endingDiffMilliseconds / 1000))

      val speedDiff = (endingSpeed - initialSpeed)
      val timeDiff = Math.abs(lastMilliseconds - secondMilliseconds) / 1000

      BigDecimal(speedDiff / timeDiff).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    val decelUDF = udf(calcSpeed(_: String, 0))
    val decelCol = decelUDF.apply(inputDF.col("positions"))

    val withSpeedDF = inputDF.withColumn("decel", decelCol.cast(DoubleType)).filter("decel<=-0.5")

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

  /**
   * Computes distance between two (lat,long) points. This implementation is based on Thaddeus Vincenty's formulas
   * for calculating distances on an ellipsoid: http://en.wikipedia.org/wiki/Vincenty%27s_formulae
   */
  def getDistance(lat1: Double, long1: Double, lat2: Double, long2: Double): Double = {
    val a = 6378137d // length of semi-major axis of the ellipsoid (radius at equator), meters
    val b = 6356752.314245 // length of semi-minor axis of the ellipsoid (radius at the poles), meters
    val f = 1 / 298.257223563 // 	flattening of the ellipsoid
    val L = Math.toRadians(long2 - long1) // 	difference in longitude of two points
    val U1 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat1))) // reduced latitude1 (latitude on the auxiliary sphere)
    val U2 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat2))) // reduced latitude2 (latitude on the auxiliary sphere)
    val sinU1 = Math.sin(U1)
    val sinU2 = Math.sin(U2)
    val cosU1 = Math.cos(U1)
    val cosU2 = Math.cos(U2)

    val iterLimit = 95
    //Parameters to be evaluated iteratively
    var cosSqAlpha = 0d
    var sinSigma = 0d
    var cos2SigmaM = 0d
    var cosSigma = 0d
    var sigma = 0d // arc length between points on the auxiliary sphere
    var lambda = L // longitude difference on the auxiliary sphere
    var prevLambda = 0d

    breakable {
      1 to iterLimit foreach { _ =>
        val sinLambda = Math.sin(lambda)
        val cosLambda = Math.cos(lambda)
        sinSigma = Math.sqrt((cosU2 * sinLambda) * (cosU2 * sinLambda) +
          (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) * (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda))
        if (sinSigma == 0) break

        cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda
        sigma = Math.atan2(sinSigma, cosSigma)
        val sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma
        cosSqAlpha = 1 - sinAlpha * sinAlpha
        cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha
        val C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha))

        prevLambda = lambda
        lambda = L + (1 - C) * f * sinAlpha * (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)))
        if (Math.abs(lambda - prevLambda) <= 1e-12) break
      }
    }
    if (sinSigma == 0 || Math.abs(lambda - prevLambda) > 1e-12) 0 else {
      val uSq = cosSqAlpha * (a * a - b * b) / (b * b)
      val A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)))
      val B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)))
      val deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) -
        B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)))
      val s = b * A * (sigma - deltaSigma) // ellipsoidal distance between the two points
      s
    }
  }

}