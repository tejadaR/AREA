package rtejada.projects.AREA.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import java.util.Calendar

class ShortestPathDemo(pp: Preprocessor, predictionsDF: DataFrame, cleanVE: (DataFrame, DataFrame)) extends Serializable {
  val vDF = cleanVE._1
  val eDF = cleanVE._2

  val prelimDF = getGoalVertex(getStartVertex(predictionsDF))

  val vArr = vDF.collect.map(row => {
    new Vertex(row.getAs[String]("nodeID"),
      row.getAs[String]("latitude").toDouble,
      row.getAs[String]("longitude").toDouble,
      0, 0, 0, "none", 0)
  })
  val eArr = eDF.collect.map(row => {
    new Edge(row.getAs[String]("LinkID"),
      row.getAs[String]("Length").toDouble,
      row.getAs[String]("src"),
      row.getAs[String]("dst"))
  })

  val startEpoch = Calendar.getInstance.getTimeInMillis

  val testRow = prelimDF.head
  val testStartV = testRow.getAs[String]("startVertex")
  val testGoalV = testRow.getAs[String]("goalVertex")

  val test = findOptimal(vArr, eArr, testStartV, testGoalV)
  val runDuration = Calendar.getInstance.getTimeInMillis - startEpoch
  println("Duration: " + runDuration + "ms")
  test.foreach(v => println(v.nodeID + ", " + v.heuristic + ", " + v.dist + ", " + v.cost + ", " + v.via + ", " + v.visited))

  private def findOptimal(vertices: Array[Vertex], edges: Array[Edge], startV: String, goalV: String) = {
    val readyVertices = initVertices(vertices, startV, goalV)
    def aStarSearch(vertices: Array[Vertex]): Array[Vertex] = {
      val curV = vertices.filter(_.visited == 0).minBy(v => v.cost)
      val relevEdges = edges.filter(e => e.src == curV.nodeID || e.dst == curV.nodeID)
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
      if (relevEdges.exists { e => (e.src == goalV || e.dst == goalV) }) updatedV else (aStarSearch(updatedV))

    }
    aStarSearch(readyVertices).filter(v => v.visited != 0)
  }

  private def initVertices(vertices: Array[Vertex], startV: String, goalV: String): Array[Vertex] = {
    val goalVertex = vertices.filter(v => v.nodeID == goalV).head
    vertices.map(v => {
      val heuristic = pp.getDistance(v.lat, v.long, goalVertex.lat, goalVertex.long) * 3.28084 //m to ft
      val distance = if (v.nodeID == startV) 0.0D else Double.MaxValue
      val cost = if (v.nodeID == startV) heuristic else Double.MaxValue
      new Vertex(v.nodeID, v.lat, v.long, heuristic, distance, cost, v.via, v.visited)
    })
  }

  case class Vertex(nodeID: String, lat: Double, long: Double, heuristic: Double,
                    dist: Double, cost: Double, via: String, visited: Integer)
  case class Edge(linkID: String, length: Double, src: String, dst: String)

  private def getStartVertex(inputDF: DataFrame): DataFrame = {
    val thresholdSpeed = 18 //meters per second
    def findStartV(positions: String, links: String) = {
      val posArraySize = positions.split("\\|").length
      val positionsUsed = Math.min(posArraySize, 60)
      val posUsedArr = positions.split("\\|").slice(0, positionsUsed)
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
        case None      => ("", "")
      }

      val latIncreasing = slowPositions._1.split(";")(0).toDouble < slowPositions._2.split(";")(0).toDouble
      val longIncreasing = slowPositions._1.split(";")(1).toDouble < slowPositions._2.split(";")(1).toDouble
      val slowTime = getMilliseconds(slowPositions._2.split(";"))

      val linkPairs = linksArr zip linksArr.tail
      val slowLinkID = linkPairs.find(pair => {
        getMilliseconds(pair._2.split(";")) > slowTime
      }) match {
        case Some(link) => link._2.split(";")(3)
        case None       => ""
      }

      val slowLink = eArr.filter(edge => edge.linkID == slowLinkID).head
      val src = vArr.filter(vertex => vertex.nodeID == slowLink.src).head
      val dst = vArr.filter(vertex => vertex.nodeID == slowLink.dst).head

      val srcCondition = (src.lat > dst.lat && latIncreasing) ||
        (src.long > dst.long && longIncreasing)

      if (srcCondition) slowLink.src else slowLink.dst
    }

    val vertexUDF = udf(findStartV(_: String, _: String))
    val vertexCol = vertexUDF.apply(inputDF.col("positions"), inputDF.col("links"))
    val withStartDF = inputDF.withColumn("startVertex", vertexCol)
    withStartDF
  }

  private def getGoalVertex(inputDF: DataFrame): DataFrame = {
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
    val withStartDF = inputDF.withColumn("goalVertex", vertexCol)
    withStartDF
  }
}