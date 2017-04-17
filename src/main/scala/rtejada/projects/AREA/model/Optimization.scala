package rtejada.projects.AREA.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import rtejada.projects.AREA.utils.Interface
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

class Optimization(spark: SparkSession, pp: Preprocessor, predictionsDF: DataFrame, verticesDF: DataFrame,
                   edgesDF: DataFrame, sizeDefDF: DataFrame, runTimeId: Long) extends Serializable {
  import spark.implicits._
  //INIT PARAMS
  val thresholdSpeed = 35 //meters per second
  val rotSavings = Map("GA" -> 0.1848, "Small" -> 0.547556, "Medium" -> 3.518044,
    "Large" -> 8.110667, "Super" -> 11.17013) // $/second
  val ptCosts = Map("GA" -> 0.2215, "Small" -> 0.492222, "Medium" -> 3.1625278,
    "Large" -> 4.8606944, "Super" -> 6.6942222) // $/second
  val fuelCost = 0.1796667 // $/second
  val crewCost = 0.271 // $/second
  val maintCost = 0.2003333 // $/second
  val acOwnershipCost = 0.132 // $/second
  val envCost = 0.6 // $/second
  val otherCost = 0.0451667 // $/second
  val sizeDefArr = sizeDefDF.collect.map(row => (row.getAs[String]("ACTypes"), row.getAs[String]("Category")))

  //VERTICES AND EDGES
  case class Vertex(nodeID: String, lat: Double, long: Double, heuristic: Double,
                    dist: Double, cost: Double, via: String, visited: Integer)
  case class Edge(linkID: String, length: Double, src: String, dst: String, isExit: Int)

  val vArr = verticesDF.collect.map(row => {
    Vertex(row.getAs[String]("nodeID"),
      row.getAs[String]("latitude").toDouble,
      row.getAs[String]("longitude").toDouble,
      0, 0, 0, "none", 0)
  })
  val eArr = edgesDF.collect.map(row => {
    Edge(row.getAs[String]("LinkID"),
      row.getAs[String]("Length").toDouble,
      row.getAs[String]("src"),
      row.getAs[String]("dst"),
      row.getAs[Int]("isExit"))
  })

  //EXECUTE AND OUTPUT
  val optimizedResults = optimize(predictionsDF, vArr, eArr)

  /** Executes optimization process: Prepares, adds paths and financials, and formats data */
  private def optimize(inputDF: DataFrame, vertices: Array[Vertex], edges: Array[Edge]) {
    val preparedDF = prepare(inputDF, vertices, edges)
    val pathDetailsDF = addPathDetails(preparedDF).cache()
    val financialDF = addFinancials(pathDetailsDF)
    val readyResults = formatResults(financialDF)

    /*financialDF.printSchema
    financialDF.drop("positions", "links").show(50, false)
    financialDF.describe("optROT", "optTT", "actualROT", "actualTT",
      "optPcost", "optFuelCost", "optCrewCost", "optMaintCost",
      "optAOCost", "optEnvCost", "optOtherCost", "actualPcost",
      "actualFuelCost", "actualCrewCost", "actualMaintCost",
      "actualAOCost", "actualEnvCost", "actualOtherCost",
      "savings", "totalOptCost", "totalActualCost").coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save("testout/results.csv")
    System.exit(0)*/
  }

  private def formatResults(inputDF: DataFrame) = {
    def avg(colName: String) = inputDF.select(mean(colName)).head().getAs[Double](0)
    val summaryResults: List[SingleRes] = List(SingleRes("optROT", avg("optROT")),
      SingleRes("optTT", avg("optTT")),
      SingleRes("actualROT", avg("actualROT")),
      SingleRes("actualTT", avg("actualTT")),
      SingleRes("optPcost", avg("optPcost")),
      SingleRes("optFuelCost", avg("optFuelCost")),
      SingleRes("optCrewCost", avg("optCrewCost")),
      SingleRes("optMaintCost", avg("optMaintCost")),
      SingleRes("optAOCost", avg("optAOCost")),
      SingleRes("optEnvCost", avg("optEnvCost")),
      SingleRes("optOtherCost", avg("optOtherCost")),
      SingleRes("actualPcost", avg("actualPcost")),
      SingleRes("actualFuelCost", avg("actualFuelCost")),
      SingleRes("actualCrewCost", avg("actualCrewCost")),
      SingleRes("actualMaintCost", avg("actualMaintCost")),
      SingleRes("actualAOCost", avg("actualAOCost")),
      SingleRes("actualEnvCost", avg("actualEnvCost")),
      SingleRes("actualOtherCost", avg("actualOtherCost")),
      SingleRes("savings", avg("savings")))
    val count = inputDF.count
    val sampleCount = if (count < 25) count else 25
    val sampleRows = inputDF.select("optROT", "optTT", "actualROT", "actualTT",
      "optPath", "actualPath", "slowV", "totalOptCost", "totalActualCost",
      "savings").sample(false, 0, sampleCount / count).collect()
    val resultStr = resultsJson(summaryResults, sampleRows)
  }

  case class SingleRes(name: String, value: Double)
  case class SampleRes(optROT: Double, optTT: Double, actualROT: Double, actualTT: Double,
                       optPath: Array[(String, Int, Double)], actualPath: Array[(String, Int, Double)],
                       slowV: String, totalOptCost: Double, totalActualCost: Double, savings: Double)

  private def resultsJson(summaryResults: List[SingleRes], sampleRows: Array[Row]) = {

  }

  /** Adds start and goal vertex */
  private def prepare(inputDF: DataFrame, vertices: Array[Vertex], edges: Array[Edge]): DataFrame = {
    val withSlow = addSlowV(inputDF)
    val withGoal = addGoalV(withSlow)
    withGoal.filter("goalV != slowV")
  }

  /** Adds starting vertex(slowV). Filters out records where slowV was not found */
  private def addSlowV(inputDF: DataFrame): DataFrame = {
    /** User-defined function, takes in account slow speed threshold and direction to find startV  **/
    def findStartV(positions: String, links: String) = {
      val posRawArr = positions.split("\\|")
      val posUsedArr = posRawArr.slice(0, Math.min(posRawArr.length, 80))
      val linksArr = links.split("\\|")

      def getMilliseconds(latLongEpochArr: Array[String]): Long = {
        val decimal = latLongEpochArr(2).split("E")(0).toDouble
        val exponential = latLongEpochArr(2).split("E")(1).toDouble
        (decimal * Math.pow(10, exponential)).toLong
      }

      val posPairs = posUsedArr zip posUsedArr.tail
      val slowPositions = posPairs.find(pair => {
        val a = pair._1; val b = pair._2
        val firstLat = a.split(";")(0).toDouble
        val firstLong = a.split(";")(1).toDouble
        val firstMilliseconds = getMilliseconds(a.split(";"))
        val secondLat = b.split(";")(0).toDouble
        val secondLong = b.split(";")(1).toDouble
        val secondMilliseconds = getMilliseconds(b.split(";"))
        val distance = pp.getDistance(firstLat, firstLong, secondLat, secondLong)
        val diffMilliSeconds = Math.abs(firstMilliseconds.toDouble - secondMilliseconds.toDouble)
        val speed = (distance / (diffMilliSeconds / 1000))
        speed < thresholdSpeed
      }) match {
        case Some(pos) => pos
        case None      => posPairs.last
      }

      val latIncreasing = slowPositions._1.split(";")(0).toDouble < slowPositions._2.split(";")(0).toDouble
      val longIncreasing = slowPositions._1.split(";")(1).toDouble < slowPositions._2.split(";")(1).toDouble
      val slowMillis = getMilliseconds(slowPositions._2.split(";"))

      val slowIndex = linksArr.indexWhere { link => getMilliseconds(link.split(";")) > slowMillis }
      val slowLinkID = if (slowIndex != -1) linksArr(slowIndex).split(";")(3) else "none"

      val slowLink = eArr.find(edge => edge.linkID == slowLinkID)
      slowLink match {
        case Some(link) => {
          val src = vArr.filter(vertex => vertex.nodeID == link.src).head
          val dst = vArr.filter(vertex => vertex.nodeID == link.dst).head
          val srcCondition = (src.lat > dst.lat && latIncreasing) ||
            (src.long > dst.long && longIncreasing)
          if (srcCondition) link.src else link.dst
        }
        case None => "none"
      }
    }

    val vertexUDF = udf(findStartV(_: String, _: String))
    val vertexCol = vertexUDF.apply(inputDF.col("positions"), inputDF.col("links"))
    val withSlowDF = inputDF.withColumn("slowV", vertexCol).filter("slowV != 'none'")
    withSlowDF
  }

  /** Adds goal vertex(goalV). Filters out records where goalV was not found */
  private def addGoalV(inputDF: DataFrame): DataFrame = {
    def findGoalV(links: String) = {
      val linksArr = links.split("\\|")
      val linkID = linksArr.last.split(";")(3)
      eArr.find(edge => { edge.linkID == linkID }) match {
        case Some(e) => e.dst
        case None    => "none"
      }
    }
    val vertexUDF = udf(findGoalV(_: String))
    val vertexCol = vertexUDF.apply(inputDF.col("links"))
    val withStartDF = inputDF.withColumn("goalV", vertexCol).filter("goalV != 'none'")
    withStartDF
  }

  /** Adds optimal and actual path details, including the link sequence, ROT and TT */
  private def addPathDetails(inputDF: DataFrame): DataFrame = {
    val optPathUDF = udf(findOptPath(_: String, _: String))
    val optPathCol = optPathUDF.apply(inputDF.col("slowV"), inputDF.col("goalV"))
    val optPathDF = inputDF.withColumn("optPath", optPathCol)

    val actualPathUDF = udf(findActualPath(_: String, _: String, _: String))
    val actualPathCol = actualPathUDF.apply(optPathDF.col("slowV"), optPathDF.col("predictedExit"), optPathDF.col("goalV"))
    val actualPathDF = optPathDF.withColumn("actualPath", actualPathCol)

    val optRotUDF = udf(findOptROT(_: Seq[Row]))
    val optRotCol = optRotUDF.apply(actualPathDF.col("optPath"))
    val optRotDF = actualPathDF.withColumn("optROT", optRotCol).filter("optROT != -1")

    val optTtUDF = udf(findOptTT(_: Seq[Row]))
    val optTtCol = optTtUDF.apply(optRotDF.col("optPath"))
    val optTtDF = optRotDF.withColumn("optTT", optTtCol).filter("optTT != -1")

    val actualRotUDF = udf(findActualROT(_: String, _: String))
    val actualRotCol = actualRotUDF.apply(optTtDF.col("slowV"), optTtDF.col("predictedExit"))
    val actualRotDF = optTtDF.withColumn("actualROT", actualRotCol).filter("actualROT != -1")

    val actualTtUDF = udf(findActualTT(_: Seq[Row]))
    val actualTtCol = actualTtUDF.apply(actualRotDF.col("actualPath"))
    val actualTtDF = actualRotDF.withColumn("actualTT", actualTtCol).filter("actualTT != -1")

    actualTtDF
  }

  /** Finds the optimal path sequence, with ID, if it's an exit, and its length: List[(String, Int, Double)] */
  private def findOptPath(slowV: String, goalV: String) = {
    val readyVertices = initVertices(vArr, slowV, goalV)

    val rawExplored = aStarSearch(readyVertices, slowV, goalV).filter(v => v.visited != 0)
    val vertexPath = tracePath(rawExplored, slowV, goalV)
    val edgePath = (vertexPath zip vertexPath.tail).map(vPairs => {
      eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
        (e.dst == vPairs._1 && e.src == vPairs._2)) match {
        case Some(e) => (e.linkID, e.isExit, e.length)
        case None    => ("none", 0, 0D)
      }
    })
    edgePath
  }

  /** Initializes vertices by adding values for: heuristic, distance, cost, visited and 'via' */
  private def initVertices(vertices: Array[Vertex], startV: String, goalV: String): Array[Vertex] = {
    val goalVertex = vertices.filter(v => v.nodeID == goalV).head
    vertices.map(v => {
      val heuristic = pp.getDistance(v.lat, v.long, goalVertex.lat, goalVertex.long) * 3.28084 //m to ft
      val distance = if (v.nodeID == startV) 0.0D else Double.MaxValue
      val cost = if (v.nodeID == startV) heuristic else Double.MaxValue
      new Vertex(v.nodeID, v.lat, v.long, heuristic, distance, cost, v.via, v.visited)
    })
  }

  /** A* search recursive algorithm implementation */
  private def aStarSearch(vertices: Array[Vertex], startV: String, goalV: String): Array[Vertex] = {
    val curV = vertices.filter(_.visited == 0).minBy(v => v.cost)
    val relevEdges = eArr.filter(e => e.src == curV.nodeID || e.dst == curV.nodeID)
    val updatedV = vertices.map(v => {
      val newVisited: Integer = if (v.nodeID == curV.nodeID) 1 else v.visited
      val isRelevant = relevEdges.exists(e => {
        e.src == v.nodeID ||
          e.dst == v.nodeID
      })
      if (isRelevant) {
        val candidateDist = relevEdges.find(e => {
          e.src == v.nodeID ||
            e.dst == v.nodeID
        }) match {
          case Some(e) => e.length + curV.dist
          case None    => Double.MaxValue
        }
        val candidateCost = v.heuristic + candidateDist
        val isLowerDist = candidateDist < v.dist
        if (isLowerDist)
          new Vertex(v.nodeID, v.lat, v.long, v.heuristic, candidateDist, candidateCost, curV.nodeID, newVisited)
        else new Vertex(v.nodeID, v.lat, v.long, v.heuristic, v.dist, v.cost, v.via, newVisited)
      } else new Vertex(v.nodeID, v.lat, v.long, v.heuristic, v.dist, v.cost, v.via, newVisited)
    })
    if (relevEdges.exists { e => (e.src == goalV || e.dst == goalV) }) updatedV.map(v => {
      if (v.nodeID == startV || v.nodeID == goalV) // mark node as visited
        Vertex(v.nodeID, v.lat, v.long, v.heuristic, v.dist, v.cost, v.via, 1)
      else v
    })
    else (aStarSearch(updatedV, startV, goalV))
  }

  /** Traces A* search results to obtain the optimal path's list of vertex IDs */
  private def tracePath(inputArr: Array[Vertex], startV: String, goalV: String) = {
    var res = List.empty[String]
    findVia(goalV)
    def findVia(nxt: String): Unit = inputArr.find(_.nodeID == nxt) match {
      case Some(v) => {
        res = v.nodeID :: res
        if (v.via != startV) findVia(v.via)
        else res = v.via :: res
      }
      case None => {
        println("path error, Vertex: " + nxt + "not found in visited array, where startV: " + startV + " and goalV: " + goalV)
        inputArr.foreach { x => println(x.nodeID + ", " + x.via + ", " + x.visited + ", " + x.heuristic) }
      }
    }
    res
  }

  /** Optimal Runway occupancy-time (ROT) in seconds */
  private def findOptROT(edgePath: Seq[Row]) = {
    val exitIndex = edgePath.indexWhere(tup => tup(1) == 1)
    val rotSeq = edgePath.slice(0, exitIndex + 1)

    if (!rotSeq.isEmpty) {
      val rotLength = rotSeq.map(_(2).asInstanceOf[Double]).reduceLeft(_ + _)
      (rotLength * 0.3048) / thresholdSpeed
    } else -1.0
  }

  /** Optimal Taxi-time(TT) in seconds */
  private def findOptTT(edgePath: Seq[Row]) = {
    val exitIndex = edgePath.indexWhere(tup => tup(1) == 1)
    val ttSeq = edgePath.slice(exitIndex + 1, edgePath.length)

    if (!ttSeq.isEmpty) {
      val ttLength = ttSeq.map(_(2).asInstanceOf[Double]).reduceLeft(_ + _)
      (ttLength * 0.3048) / 11.83 // 11.83 m/s taxiing speed
    } else -1.0
  }

  /** 'Actual' Path from ML-predicted exit(used as surrogate) to goal */
  private def findActualPath(slowV: String, predictedExit: String, goalV: String) = {
    val minLengthWithID = getMinLengthWithID(slowV, predictedExit)

    val readyVertices = initVertices(vArr, minLengthWithID._2, goalV)
    val rawExplored = aStarSearch(readyVertices, minLengthWithID._2, goalV).filter(v => v.visited != 0)
    val vertexPath = tracePath(rawExplored, minLengthWithID._2, goalV)
    val edgePath = (vertexPath zip vertexPath.tail).map(vPairs => {
      eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
        (e.dst == vPairs._1 && e.src == vPairs._2)) match {
        case Some(e) => (e.linkID, e.isExit, e.length)
        case None    => ("none", 0, 0D)
      }
    })
    edgePath
  }

  /**
   * Utility method for finding the exit vertex closest to the runway.
   * Used in determining ROT (length/speed) and in setting the start vertex for A*
   */
  private def getMinLengthWithID(slowV: String, predictedExit: String): (Double, String) = {
    val exitEdgeInfo = eArr.find(e => e.linkID == predictedExit) match {
      case Some(e) => (e.src, e.dst, e.length) // will add exit length to distance
      case None    => ("none", "none", 0D)
    }
    val exitSrcCoords = vArr.find(_.nodeID == exitEdgeInfo._1) match {
      case Some(v) => (v.lat, v.long, v.nodeID)
      case None    => (0D, 0D, "none")
    }
    val exitDstCoords = vArr.find(_.nodeID == exitEdgeInfo._2) match {
      case Some(v) => (v.lat, v.long, v.nodeID)
      case None    => (0D, 0D, "none")
    }
    vArr.find(_.nodeID == slowV) match {
      case Some(v) => {
        val srcDist = pp.getDistance(v.lat, v.long, exitSrcCoords._1, exitSrcCoords._2)
        val dstDist = pp.getDistance(v.lat, v.long, exitDstCoords._1, exitDstCoords._2)
        if (srcDist < dstDist) (srcDist + exitEdgeInfo._3, exitSrcCoords._3) else (dstDist + exitEdgeInfo._3, exitDstCoords._3)
      }
      case None => (0D, "none")
    }
  }

  /** 'Actual' Runway occupancy-time (ROT) in seconds */
  private def findActualROT(startV: String, predictedExit: String) = {
    val minLengthWithID = getMinLengthWithID(startV, predictedExit)
    if (minLengthWithID._2 != "none") minLengthWithID._1 / thresholdSpeed else -1D
  }

  /** 'Actual' Taxi-time(TT) in seconds */
  private def findActualTT(edgePath: Seq[Row]) = if (edgePath.size > 1) {
    val ttLength = edgePath.drop(1).map(_(2).asInstanceOf[Double]).reduceLeft(_ + _)
    (ttLength * 0.3048) / 11.83 // 11.83 m/s taxiing speed
  } else -1.0

  /** Adds financial costs and saving values related to optimal v.s. actual */
  private def addFinancials(inputDF: DataFrame) = {
    val updatedTypeUDF = udf((acType: String) => sizeDefArr.find(p => p._1 == acType) match {
      case Some(sizeDef) => sizeDef
      case None          => (acType, "GA")
    })
    val updatedTypeDF = inputDF.withColumn("aircraftType", updatedTypeUDF.apply(inputDF.col("aircraftType")))

    val optPcostUDF = udf((aircraftType: String, rot: Double, tt: Double) => {
      val pCost = ptCosts.get(aircraftType) match {
        case Some(c) => c
        case None    => 0D
      }
      (rot + tt) * pCost
    })
    val optPcostCol = optPcostUDF.apply(updatedTypeDF.col("aircraftType._2"), updatedTypeDF.col("optROT"), updatedTypeDF.col("optTT"))
    val optPcost = updatedTypeDF.withColumn("optPcost", optPcostCol)

    val optFuelCost = optPcost.withColumn("optFuelCost", (optPcost.col("optROT") + optPcost.col("optTT")) * fuelCost)
    val optCrewCost = optFuelCost.withColumn("optCrewCost", (optFuelCost.col("optROT") + optFuelCost.col("optTT")) * crewCost)
    val optMaintCost = optCrewCost.withColumn("optMaintCost", (optCrewCost.col("optROT") + optCrewCost.col("optTT")) * maintCost)
    val optAOCost = optMaintCost.withColumn("optAOCost", (optMaintCost.col("optROT") + optMaintCost.col("optTT")) * acOwnershipCost)
    val optEnvCost = optAOCost.withColumn("optEnvCost", (optAOCost.col("optROT") + optAOCost.col("optTT")) * envCost)
    val optOtherCost = optEnvCost.withColumn("optOtherCost", (optEnvCost.col("optROT") + optEnvCost.col("optTT")) * otherCost)

    val actualPcostUDF = udf((aircraftType: String, rot: Double, tt: Double) => {
      val pCost = ptCosts.get(aircraftType) match {
        case Some(c) => c
        case None    => 0D
      }
      (rot + tt) * pCost
    })
    val actualPcostCol = actualPcostUDF.apply(optOtherCost.col("aircraftType._2"), optOtherCost.col("actualROT"), optOtherCost.col("actualTT"))
    val actualPcost = optOtherCost.withColumn("actualPcost", actualPcostCol)

    val actualFuelCost = actualPcost.withColumn("actualFuelCost", (actualPcost.col("actualROT") + actualPcost.col("actualTT")) * fuelCost)
    val actualCrewCost = actualFuelCost.withColumn("actualCrewCost", (actualFuelCost.col("actualROT") + actualFuelCost.col("actualTT")) * crewCost)
    val actualMaintCost = actualCrewCost.withColumn("actualMaintCost", (actualCrewCost.col("actualROT") + actualCrewCost.col("optTT")) * maintCost)
    val actualAOCost = actualMaintCost.withColumn("actualAOCost", (actualMaintCost.col("actualROT") + actualMaintCost.col("actualTT")) * acOwnershipCost)
    val actualEnvCost = actualAOCost.withColumn("actualEnvCost", (actualAOCost.col("actualROT") + actualAOCost.col("actualTT")) * envCost)
    val actualOtherCost = actualEnvCost.withColumn("actualOtherCost", (actualEnvCost.col("actualROT") + actualEnvCost.col("actualTT")) * otherCost)

    val savingsUDF = udf((aircraftType: String, deltaROT: Double) => {
      val savings = rotSavings.get(aircraftType) match {
        case Some(s) => s
        case None    => 0D
      }
      deltaROT * savings
    })
    val savingsCol = savingsUDF.apply(actualOtherCost.col("aircraftType._2"), actualOtherCost.col("actualROT") - actualOtherCost.col("optROT"))
    val savingsDF = actualOtherCost.withColumn("savings", savingsCol)

    val totalOptCost = savingsDF.withColumn("totalOptCost", savingsDF.col("optPcost") +
      savingsDF.col("optFuelCost") + savingsDF.col("optCrewCost") +
      savingsDF.col("optMaintCost") + savingsDF.col("optAOCost") +
      savingsDF.col("optEnvCost") + savingsDF.col("optOtherCost"))

    val totalActualCost = totalOptCost.withColumn("totalActualCost", totalOptCost.col("actualPcost") +
      totalOptCost.col("actualFuelCost") + totalOptCost.col("actualCrewCost") +
      totalOptCost.col("actualMaintCost") + totalOptCost.col("actualAOCost") +
      totalOptCost.col("actualEnvCost") + totalOptCost.col("actualOtherCost"))

    totalActualCost
  }

}