package rtejada.projects.AREA.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import rtejada.projects.AREA.utils.Interface
//import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json.JsonDSL._

class RunOptimization(spark: SparkSession, pp: Preprocessor, predictionsDF: DataFrame, verticesDF: DataFrame,
                      edgesDF: DataFrame, sizeDefDF: DataFrame, runTimeId: Long, airportCode: String) extends Serializable {
  import spark.implicits._

  //INIT PARAMS
  val thresholdSpeed = 50D //meters per second
  val taxiingSpeed = 11.83D //meters per second
  val rotBenefit = 1.11D
  val airportSavingsAdjustments = airportCode match {
    case "KPHX" => 0.25D
    case "KATL" => 0.291667D
    case "KBWI" => 0.125D
    case _      => 0D
  }

  val ptCosts = Map("GA" -> 0.26, "Small" -> 0.66, "Medium" -> 2.03,
    "Large" -> 3.75, "Super" -> 6.83) // $/second
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
  Interface.output(optimizedResults, "optimization" + runTimeId + ".json")

  /** Executes optimization process: Prepares, adds paths and financials, and formats data */
  private def optimize(inputDF: DataFrame, vertices: Array[Vertex], edges: Array[Edge]): String = {
    val preparedDF = prepare(inputDF, vertices, edges)
    val pathDetailsDF = addPathDetails(preparedDF).cache()
    val financialDF = addFinancials(pathDetailsDF)
    val readyResults = formatResults(financialDF)
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
    readyResults

    //financialDF.printSchema
    //financialDF.drop("positions", "links").show(50, false)
  }

  /** Adds start and goal vertex */
  private def prepare(inputDF: DataFrame, vertices: Array[Vertex], edges: Array[Edge]): DataFrame = {
    val withSlow = addSlowV(inputDF)
    val withGoal = addGoalV(withSlow)
    withGoal.filter("goalV != slowV").filter(r => {
      val goalV = r.getAs[String]("goalV")
      val slowV = r.getAs[String]("slowV")
      !eArr.exists(e => (e.src == goalV && e.dst == slowV) || (e.src == slowV && e.dst == goalV))
    })
  }

  /** Adds starting vertex(slowV). Filters out records where slowV was not found */
  private def addSlowV(inputDF: DataFrame): DataFrame = {
    /** User-defined function, takes in account slow speed threshold to find slowV  **/
    def findStartV(positions: String, links: String) = {
      val posRawArr = positions.split("\\|")
      val linksRawArr = links.split("\\|")

      def avgDistLinkToPos(linkID: String, posCoords: (Double, Double)) = {
        val edge = eArr.filter(_.linkID == linkID)
        if (!edge.isEmpty) {
          val src = vArr.filter(_.nodeID == edge.head.src).apply(0)
          val dst = vArr.filter(_.nodeID == edge.head.dst).apply(0)
          val srcDist = Math.sqrt(Math.pow(src.lat - posCoords._1, 2) + Math.pow(src.long - posCoords._2, 2))
          val dstDist = Math.sqrt(Math.pow(dst.lat - posCoords._1, 2) + Math.pow(dst.long - posCoords._2, 2))
          (srcDist + dstDist) / 2
        } else 0
      }

      def getMilliseconds(latLongEpochArr: Array[String]): Long = {
        val decimal = latLongEpochArr(2).split("E")(0).toDouble
        val exponential = latLongEpochArr(2).split("E")(1).toDouble
        (decimal * Math.pow(10, exponential)).toLong
      }
      val posPairs = posRawArr zip posRawArr.tail

      val slowPosition = posPairs.find(pair => {
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
        case Some(pPair) => (pPair._2.split(";")(0).toDouble, pPair._2.split(";")(1).toDouble)
        case None        => (posPairs.last._2.split(";")(0).toDouble, posPairs.last._2.split(";")(1).toDouble)
      }
      val linkPairs = linksRawArr zip linksRawArr.tail
      linkPairs.find(linkPair => {
        val firstID = linkPair._1.split(";")(3)
        val secondID = linkPair._2.split(";")(3)
        val firstDist = avgDistLinkToPos(firstID, slowPosition)
        val secondDist = avgDistLinkToPos(secondID, slowPosition)
        if (firstDist == 0 || secondDist == 0) false else secondDist > firstDist
      }) match {
        case Some(linkPair) => {
          if (eArr.filter(_.linkID == linkPair._1.split(";")(3)).isEmpty ||
            eArr.filter(_.linkID == linkPair._2.split(";")(3)).isEmpty) "none"
          else {
            val firstEdge = eArr.filter(_.linkID == linkPair._1.split(";")(3)).apply(0)
            val secondEdge = eArr.filter(_.linkID == linkPair._2.split(";")(3)).apply(0)
            if (firstEdge.src == secondEdge.src || firstEdge.src == secondEdge.dst) firstEdge.src else firstEdge.dst
          }
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
      val lastID = linksArr.last.split(";")(3)
      val beforeID = linksArr.dropRight(1).last.split(";")(3)
      val lastEdgeArr = eArr.filter(_.linkID == lastID)
      val beforeEdgeArr = eArr.filter(_.linkID == beforeID)
      if (!lastEdgeArr.isEmpty && !beforeEdgeArr.isEmpty) {
        val lastEdge = lastEdgeArr.apply(0)
        val beforeEdge = beforeEdgeArr.apply(0)
        if (lastEdge.src == beforeEdge.src || lastEdge.src == beforeEdge.dst) lastEdge.dst
        else if (lastEdge.dst == beforeEdge.src || lastEdge.dst == beforeEdge.dst) lastEdge.src
        else "none"
      } else "none"
    }
    val vertexUDF = udf(findGoalV(_: String))
    val vertexCol = vertexUDF.apply(inputDF.col("links"))
    val withStartDF = inputDF.withColumn("goalV", vertexCol).filter("goalV != 'none'")
    withStartDF
  }

  /** Adds optimal and actual path details, including the link sequence, ROT and TT */
  private def addPathDetails(inputDF: DataFrame): DataFrame = {
    val optPathUDF = udf(findOptPath(_: String, _: String, _: String))
    val optPathCol = optPathUDF.apply(inputDF.col("slowV"), inputDF.col("goalV"), inputDF.col("links"))
    val optPathDF = inputDF.withColumn("optPath", optPathCol)

    val actualPathUDF = udf(findActualPath(_: String, _: String, _: String, _: String))
    val actualPathCol = actualPathUDF.apply(optPathDF.col("slowV"), optPathDF.col("predictedExit"), optPathDF.col("goalV"), optPathDF.col("links"))
    val actualPathDF = optPathDF.withColumn("actualPath", actualPathCol)

    val optRotUDF = udf(findOptROT(_: Seq[Row]))
    val optRotCol = optRotUDF.apply(actualPathDF.col("optPath"))
    val optRotDF = actualPathDF.withColumn("optROT", optRotCol).filter("optROT != -1")

    val optTtUDF = udf(findOptTT(_: Seq[Row]))
    val optTtCol = optTtUDF.apply(optRotDF.col("optPath"))
    val optTtDF = optRotDF.withColumn("optTT", optTtCol).filter("optTT != -1")

    val actualRotUDF = udf(findActualROT(_: Seq[Row]))
    val actualRotCol = actualRotUDF.apply(optTtDF.col("actualPath"))
    val actualRotDF = optTtDF.withColumn("actualROT", actualRotCol).filter("actualROT != -1")

    val actualTtUDF = udf(findActualTT(_: Seq[Row]))
    val actualTtCol = actualTtUDF.apply(actualRotDF.col("actualPath"))
    val actualTtDF = actualRotDF.withColumn("actualTT", actualTtCol).filter("actualTT != -1")

    actualTtDF
  }

  /** Finds the optimal path sequence, with ID, if it's an exit, and its length: List[(String, Int, Double)] */
  private def findOptPath(slowV: String, goalV: String, links: String) = {
    val readyVertices = initVertices(vArr, slowV, goalV, slowV, links)

    val rawExplored = aStarSearch(readyVertices, slowV, goalV).filter(v => v.visited != 0)
    val vertexPath = tracePath(rawExplored, slowV, goalV)
    val edgePath = (vertexPath zip vertexPath.tail).map(vPairs => {
      eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
        (e.dst == vPairs._1 && e.src == vPairs._2)) match {
        case Some(e) => {
          val slowVassocExit = eArr.filter(_.isExit == 1).exists { e => e.src == slowV || e.dst == slowV }
          if (vPairs._1 == slowV && slowVassocExit) (e.linkID, 1, e.length)
          else (e.linkID, e.isExit, e.length)
        }
        case None => ("none", 0, 0D)
      }
    })
    edgePath
  }

  /** Initializes vertices by adding values for: heuristic, distance, cost, visited and 'via' */
  private def initVertices(vertices: Array[Vertex], startV: String, goalV: String, slowV: String, links: String): Array[Vertex] = {
    val linkArr = links.split("\\|")
    val slowLinkID = linkArr.find(l => {
      val currLinkID = l.split(";")(3)
      val filteredArr = eArr.filter(_.linkID == currLinkID)
      if (filteredArr.isEmpty) {
        false
      } else {
        val curEdge = filteredArr.head
        curEdge.src == slowV || curEdge.dst == slowV
      }
    })
    val avoidV = slowLinkID match {
      case Some(linkStr) => {
        val slowID = linkStr.split(";")(3)
        val filteredArr = eArr.filter(_.linkID == slowID)
        if (filteredArr.isEmpty) {
          "none"
        } else {
          val slowEdge = filteredArr.head
          if (slowEdge.src == slowV) slowEdge.dst else slowEdge.src
        }
      }
      case None => "none"
    }

    val goalVertex = vertices.filter(v => v.nodeID == goalV).head

    val res = vertices.map(v => {
      val heuristic = pp.getDistance(v.lat, v.long, goalVertex.lat, goalVertex.long) * 3.28084 //m to ft
      val distance = if (v.nodeID == startV) 0.0D else Double.MaxValue
      val cost = if (v.nodeID == startV) heuristic else Double.MaxValue
      new Vertex(v.nodeID, v.lat, v.long, heuristic, distance, cost, v.via, v.visited)
    })

    if (eArr.exists(e => ((e.src == startV && e.dst == goalV) ||
      (e.dst == startV && e.src == goalV))) || startV == avoidV) res
    else {
      res.filter(_.nodeID != avoidV)
    }
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
      if (v.nodeID == startV || v.nodeID == goalV) // mark start & goal as visited
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
        println("path error, Vertex: " + nxt + " not found in visited array, where startV: " + startV + " and goalV: " + goalV)
        inputArr.foreach { x => println(x.nodeID + ", " + x.via + ", " + x.visited + ", " + x.heuristic) }
        System.exit(0)
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
      (ttLength * 0.3048) / taxiingSpeed // 11.83 m/s taxiing speed
    } else -1.0
  }

  /** 'Actual' Path from ML-predicted exit(used as surrogate) to goal */
  private def findActualPath(slowV: String, predictedExit: String, goalV: String, links: String) = {
    val minLengthWithID = getMinLengthWithID(slowV, predictedExit)

    val readyVerticesRot = initVertices(vArr, slowV, minLengthWithID._1, slowV, links)
    val ePathRot = if (slowV != minLengthWithID._1) {
      val rawExplRot = aStarSearch(readyVerticesRot, slowV, minLengthWithID._1).filter(v => v.visited != 0)
      val vPathRot = tracePath(rawExplRot, slowV, minLengthWithID._1)
      (vPathRot zip vPathRot.tail).map(vPairs => {
        eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
          (e.dst == vPairs._1 && e.src == vPairs._2)) match {
          case Some(e) => {
            val slowVassocExit = eArr.filter(_.isExit == 1).exists { e => e.src == slowV || e.dst == slowV }
            if (vPairs._1 == slowV && slowVassocExit) (e.linkID, 1, e.length)
            else (e.linkID, e.isExit, e.length)
          }
          case None => ("none", 0, 0D)
        }
      })
    } else List.empty[(String, Int, Double)]

    val readyVerticesTT = initVertices(vArr, minLengthWithID._2, goalV, slowV, links)
    val rawExplTT = aStarSearch(readyVerticesTT, minLengthWithID._2, goalV).filter(_.visited != 0)
    val vPathTT = tracePath(rawExplTT, minLengthWithID._2, goalV)
    val ePathTT = (vPathTT zip vPathTT.tail).map(vPairs => {
      eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
        (e.dst == vPairs._1 && e.src == vPairs._2)) match {
        case Some(e) => {
          val slowVassocExit = eArr.filter(_.isExit == 1).exists { e => e.src == slowV || e.dst == slowV }
          if (vPairs._1 == slowV && slowVassocExit) (e.linkID, 1, e.length)
          else (e.linkID, e.isExit, e.length)
        }
        case None => ("none", 0, 0D)
      }
    })
    val exitEdge = eArr.filter(_.linkID == predictedExit).head

    (ePathRot ++ ((exitEdge.linkID, 1, exitEdge.length) :: ePathTT))
  }

  /**
   * Utility method for finding the exit vertex closest to the runway.
   * Used in determining ROT (length/speed) and in setting the start vertex for A*
   *
   * Returns: (closestExitNode, otherExitNode)
   */
  private def getMinLengthWithID(slowV: String, predictedExit: String): (String, String) = {
    val exitEdgeNodes = eArr.find(e => e.linkID == predictedExit) match {
      case Some(e) => (e.src, e.dst) // will add exit length to distance
      case None    => ("none", "none")
    }
    val exitSrcInfo = vArr.find(_.nodeID == exitEdgeNodes._1) match {
      case Some(v) => (v.lat, v.long, v.nodeID)
      case None    => (0D, 0D, "none")
    }
    val exitDstInfo = vArr.find(_.nodeID == exitEdgeNodes._2) match {
      case Some(v) => (v.lat, v.long, v.nodeID)
      case None    => (0D, 0D, "none")
    }
    vArr.find(_.nodeID == slowV) match {
      case Some(v) => {
        val srcDist = pp.getDistance(v.lat, v.long, exitSrcInfo._1, exitSrcInfo._2)
        val dstDist = pp.getDistance(v.lat, v.long, exitDstInfo._1, exitDstInfo._2)
        if (srcDist < dstDist) (exitSrcInfo._3, exitDstInfo._3) else (exitDstInfo._3, exitSrcInfo._3)
      }
      case None => ("none", "none")
    }
  }

  /** 'Actual' Runway occupancy-time (ROT) in seconds */
  private def findActualROT(edgePath: Seq[Row]) = {
    val exitIndex = edgePath.indexWhere(tup => tup(1) == 1)
    val rotSeq = edgePath.slice(0, exitIndex + 1)

    if (!rotSeq.isEmpty) {
      val rotLength = rotSeq.map(_(2).asInstanceOf[Double]).reduceLeft(_ + _)
      (rotLength * 0.3048) / thresholdSpeed // 
    } else -1.0
  }

  /** 'Actual' Taxi-time(TT) in seconds */
  private def findActualTT(edgePath: Seq[Row]) = {
    val exitIndex = edgePath.indexWhere(tup => tup(1) == 1)
    val ttSeq = edgePath.slice(exitIndex + 1, edgePath.length)

    if (!ttSeq.isEmpty) {
      val ttLength = ttSeq.map(_(2).asInstanceOf[Double]).reduceLeft(_ + _)
      (ttLength * 0.3048) / taxiingSpeed // 11.83 m/s taxiing speed
    } else -1.0

  }

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
    val actualMaintCost = actualCrewCost.withColumn("actualMaintCost", (actualCrewCost.col("actualROT") + actualCrewCost.col("actualTT")) * maintCost)
    val actualAOCost = actualMaintCost.withColumn("actualAOCost", (actualMaintCost.col("actualROT") + actualMaintCost.col("actualTT")) * acOwnershipCost)
    val actualEnvCost = actualAOCost.withColumn("actualEnvCost", (actualAOCost.col("actualROT") + actualAOCost.col("actualTT")) * envCost)
    val actualOtherCost = actualEnvCost.withColumn("actualOtherCost", (actualEnvCost.col("actualROT") + actualEnvCost.col("actualTT")) * otherCost)

    val savingsDF = actualOtherCost.withColumn("savings", (actualOtherCost.col("actualROT") - actualOtherCost.col("optROT")) * airportSavingsAdjustments * rotBenefit)

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

  /** Formats summary and sample results into a JSON string */
  private def formatResults(inputDF: DataFrame) = {
    def avg(colName: String) = inputDF.select(mean(colName)).head().getAs[Double](0)
    val count = inputDF.count.toDouble
    val sampleCount = if (count < 50) count else 50D

    println("count: " + count)
    println("sampleCount" + sampleCount)
    val sampleRows = inputDF.select("aircraftType", "optROT", "optTT", "actualROT", "actualTT",
      "optPath", "actualPath", "slowV", "totalOptCost", "totalActualCost",
      "savings", "predictedExit").sample(false, sampleCount / count).limit(50).collect().toList
    //sampleRows.printSchema()
    //sampleRows.show(50, false)

    val json =
      ("optROT" -> avg("optROT")) ~
        ("optTT" -> avg("optTT")) ~
        ("actualROT" -> avg("actualROT")) ~
        ("actualTT" -> avg("actualTT")) ~
        ("optPcost" -> avg("optPcost")) ~
        ("optFuelCost" -> avg("optFuelCost")) ~
        ("optCrewCost" -> avg("optCrewCost")) ~
        ("optMaintCost" -> avg("optMaintCost")) ~
        ("optAOCost" -> avg("optAOCost")) ~
        ("optEnvCost" -> avg("optEnvCost")) ~
        ("optOtherCost" -> avg("optOtherCost")) ~
        ("actualPcost" -> avg("actualPcost")) ~
        ("actualFuelCost" -> avg("actualFuelCost")) ~
        ("actualCrewCost" -> avg("actualCrewCost")) ~
        ("actualMaintCost" -> avg("actualMaintCost")) ~
        ("actualAOCost" -> avg("actualAOCost")) ~
        ("actualEnvCost" -> avg("actualEnvCost")) ~
        ("actualOtherCost" -> avg("actualOtherCost")) ~
        ("savings" -> avg("savings")) ~ ("samples" -> sampleRows.map { sR =>
          ("aircraftType" -> sR.getAs[Row]("aircraftType")(0).toString.concat(
            ", " + sR.getAs[Row]("aircraftType")(1).toString)) ~
            ("optROT" -> sR.getAs[Double]("optROT")) ~
            ("optTT" -> sR.getAs[Double]("optTT")) ~
            ("actualROT" -> sR.getAs[Double]("actualROT")) ~
            ("actualTT" -> sR.getAs[Double]("actualTT")) ~
            ("slowV" -> sR.getAs[String]("slowV")) ~
            ("totalOptCost" -> sR.getAs[Double]("totalOptCost")) ~
            ("totalActualCost" -> sR.getAs[Double]("totalActualCost")) ~
            ("savings" -> sR.getAs[Double]("savings")) ~
            ("predictedExit" -> sR.getAs[String]("predictedExit")) ~
            ("optPath" -> sR.getAs[Seq[Row]]("optPath").map { opR =>
              ("linkID" -> opR.getAs[String](0)) ~
                ("isExit" -> opR.getAs[Int](1)) ~
                ("length" -> opR.getAs[Double](2))
            }) ~
            ("actualPath" -> sR.getAs[Seq[Row]]("actualPath").map { apR =>
              ("linkID" -> apR.getAs[String](0)) ~
                ("isExit" -> apR.getAs[Int](1)) ~
                ("length" -> apR.getAs[Double](2))
            })
        })

    pretty(render(json))
  }

}