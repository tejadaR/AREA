package rtejada.projects.AREA.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import rtejada.projects.AREA.utils.Interface

class RunOptimization(pp: Preprocessor, predictionsDF: DataFrame, verticesDF: DataFrame, edgesDF: DataFrame, sizeDefDF: DataFrame, runTimeId: Long) extends Serializable {

  val thresholdSpeed = 18 //meters per second
  val ptCosts = Map("GA" -> 0.2215, "Small" -> 0.492222,
    "Medium" -> 3.1625278, "Large" -> 4.8606944, "Super" -> 6.6942222) // $/second
  val rotSavings = Map("GA" -> 0.1848, "Small" -> 0.547556,
    "Medium" -> 3.518044, "Large" -> 8.110667, "Super" -> 11.17013) // $/second
  val taxiingCost = 0.8281667 // $/second
  val envCost = 0.6 // $/second

  val sizeDefArr = sizeDefDF.collect.map { row => (row.getAs[String]("ACTypes"), row.getAs[String]("Category")) }

  val prelimDF = getGoalVertex(getStartVertex(predictionsDF))

  val vArr = verticesDF.collect.map(row => {
    new Vertex(row.getAs[String]("nodeID"),
      row.getAs[String]("latitude").toDouble,
      row.getAs[String]("longitude").toDouble,
      0, 0, 0, "none", 0)
  })
  val eArr = edgesDF.collect.map(row => {
    new Edge(row.getAs[String]("LinkID"),
      row.getAs[String]("Length").toDouble,
      row.getAs[String]("src"),
      row.getAs[String]("dst"),
      row.getAs[Int]("isExit"))
  })

  val optDF = optimize(prelimDF).drop("positions", "links")
  optDF.printSchema
  optDF.show(false)
  val num = optDF.count
  val avgOptCost = optDF.select("optCost").collect.map(row => row.getAs[Double]("optCost")).reduceLeft(_ + _) / num
  println("avgOptCost: " + avgOptCost)
  val avgActualCost = optDF.select("actualCost").collect.map(row => row.getAs[Double]("actualCost")).reduceLeft(_ + _) / num
  println("avgActualCost: " + avgActualCost)

  val sampleArr = optDF.sample(false, 0.1).select("aircraftType", "optDetails", "actualDetails", "optCost", "actualCost").collect.map(row => {
    val acType = row.getAs[Row]("aircraftType")(0).asInstanceOf[String] + "-" + row.getAs[Row]("aircraftType")(1).asInstanceOf[String]
    val optPath = row.getAs[Row]("optDetails")(2).asInstanceOf[Seq[Row]].map(_(0).asInstanceOf[String]).mkString(";")
    val actualPath = row.getAs[Row]("actualDetails")(2).asInstanceOf[Seq[String]].mkString(";")
    (acType, optPath, actualPath, row.getAs[Double]("optCost"), row.getAs[Double]("actualCost"))
  })
  val optOutStr = sampleArr.mkString("\n") + System.lineSeparator() + "totals:" + System.lineSeparator() + "" + avgOptCost + "," + avgActualCost + ""
  Interface.output(optOutStr, "optimization" + runTimeId + ".txt")

  private def optimize(inputDF: DataFrame): DataFrame = {
    val detailedDF = addAllDetails(inputDF)
    val costsDF = addCosts(detailedDF)
    //val withSavingsDF = addSavings(costsDF)
    costsDF
    //withSavingsDF
  }

  private def addSavings(inputDF: DataFrame): DataFrame = {

    val rResultUDF = udf((size: String, optDetails: Row) => rotSavings.get(size) match {
      case Some(c) => c * optDetails(0).asInstanceOf[Double]
      case None    => 0D
    })
    val rResDF = inputDF.withColumn("rRes", rResultUDF.apply(inputDF.col("aircraftType._2"), inputDF.col("optDetails")))
    rResDF.show(50)

    val aResultUDF = udf((size: String, actualDetails: Row) => rotSavings.get(size) match {
      case Some(c) => c * actualDetails(0).asInstanceOf[Double]
      case None    => 0D
    })
    val aResDF = rResDF.withColumn("aRes", aResultUDF.apply(rResDF.col("aircraftType._2"), rResDF.col("actualDetails")))
    aResDF.show(50)

    aResDF
  }

  private def addCosts(inputDF: DataFrame): DataFrame = {
    val updatedTypeUDF = udf((acType: String) => sizeDefArr.find(p => p._1 == acType) match {
      case Some(sizeDef) => sizeDef
      case None          => (acType, "none")
    })
    val updatedTypeDF = inputDF.withColumn("aircraftType", updatedTypeUDF.apply(inputDF.col("aircraftType")))

    val optCostUDF = udf(getOptCost(_: Row, _: Row))
    val optCostCol = optCostUDF.apply(updatedTypeDF.col("aircraftType"), updatedTypeDF.col("optDetails"))
    val withOptCostDF = updatedTypeDF.withColumn("optCost", optCostCol)

    val actualCostUDF = udf(getActualCost(_: Row, _: Row))
    val actualCostCol = actualCostUDF.apply(withOptCostDF.col("aircraftType"), withOptCostDF.col("actualDetails"))
    val withActualCostDF = withOptCostDF.withColumn("actualCost", actualCostCol)
    withActualCostDF
  }

  private def getOptCost(aircraftType: Row, optDetails: Row) = {
    val pCost = ptCosts.get(aircraftType(1).toString) match {
      case Some(c) => c
      case None    => 0D
    }
    val totalCostRate = pCost + taxiingCost + envCost
    (optDetails(0).asInstanceOf[Double] + optDetails(1).asInstanceOf[Double]) * totalCostRate //(ROT+TT)*totalcost
  }

  private def getActualCost(aircraftType: Row, actualDetails: Row) = {
    val pCost = ptCosts.get(aircraftType(1).toString) match {
      case Some(c) => c
      case None    => 0D
    }
    val totalCostRate = pCost + taxiingCost + envCost
    (actualDetails(0).asInstanceOf[Double] + actualDetails(1).asInstanceOf[Double]) * totalCostRate //(ROT+TT)*totalcost
  }

  private def addAllDetails(inputDF: DataFrame): DataFrame = {
    val optDetailsUDF = udf(getOptDetails(_: String, _: String))
    val optDetailsCol = optDetailsUDF.apply(inputDF.col("startVertex"), inputDF.col("goalVertex"))
    val optDetailsDF = inputDF.withColumn("optDetails", optDetailsCol).filter(row => row.getAs[Row]("optDetails")(0) != -1.0)

    val actualDetailsUDF = udf(getActualDetails(_: String, _: String, _: Row))
    val actualDetailsCol = actualDetailsUDF.apply(optDetailsDF.col("links"), optDetailsDF.col("exit"), optDetailsDF.col("optDetails"))
    val actualDetailsDF = optDetailsDF.withColumn("actualDetails", actualDetailsCol).filter(row => row.getAs[Row]("actualDetails")(0) != -1.0)

    actualDetailsDF
  }

  private def getActualDetails(links: String, exit: String, optDetails: Row) = {
    val slowLink = optDetails(2).asInstanceOf[Seq[Row]](0)(0)
    val linkIdArr = links.split("\\|").map(_.split(";")(3))
    val slowLinkIndex = linkIdArr.indexWhere(_ == slowLink)

    val exitIndex = linkIdArr.indexWhere(_ == exit)

    val rotSeq =
      if (slowLinkIndex == -1 || exitIndex < slowLinkIndex) Array.empty[String]
      else linkIdArr.slice(slowLinkIndex, exitIndex + 1)
    val ttSeq = linkIdArr.slice(exitIndex + 1, linkIdArr.length)

    val rotLength = if (!rotSeq.isEmpty) rotSeq.map(id => eArr.find(_.linkID == id) match {
      case Some(e) => e.length
      case None    => 0D
    }).reduceLeft(_ + _)
    else 0D
    val rotSeconds = (rotLength * 0.3048) / thresholdSpeed

    val ttLength = ttSeq.map(id => eArr.find(_.linkID == id) match {
      case Some(e) => e.length
      case None    => 0D
    }).reduceLeft(_ + _)
    val ttSeconds = (ttLength * 0.3048) / 11 //taxiing speed

    (rotSeconds, ttSeconds, linkIdArr)
  }

  private def getOptDetails(startV: String, goalV: String) = {
    val readyVertices = initVertices(vArr, startV, goalV)
    def aStarSearch(vertices: Array[Vertex]): Array[Vertex] = {
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
      else (aStarSearch(updatedV))
    }
    val rawExplored = aStarSearch(readyVertices).filter(v => v.visited != 0)
    val vertexPath = tracePath(rawExplored, startV, goalV)
    val edgePath = (vertexPath zip vertexPath.tail).map(vPairs => {
      eArr.find(e => (e.src == vPairs._1 && e.dst == vPairs._2) ||
        (e.dst == vPairs._1 && e.src == vPairs._2)) match {
        case Some(e) => (e.linkID, e.isExit, e.length)
        case None    => ("none", 0, 0D)
      }
    })

    val exitIndex = edgePath.indexWhere(tup => tup._2 == 1)
    val rotSeq = edgePath.slice(0, exitIndex + 1)
    val ttSeq = edgePath.slice(exitIndex + 1, edgePath.length)

    val rotSeconds = if (!rotSeq.isEmpty) {
      val rotLength = rotSeq.map(_._3).reduceLeft(_ + _)
      (rotLength * 0.3048) / thresholdSpeed
    } else -1.0

    val ttSeconds = if (!ttSeq.isEmpty) {
      val ttLength = ttSeq.map(_._3).reduceLeft(_ + _)
      (ttLength * 0.3048) / 11.83 // 11.83 m/s taxiing speed
    } else -1.0

    (rotSeconds, ttSeconds, edgePath)
  }

  private def tracePath(inputArr: Array[Vertex], startV: String, goalV: String) = {
    var res = List.empty[String]
    findVia(goalV)
    def findVia(nxt: String): Unit = inputArr.find(v => v.nodeID == nxt) match {
      case Some(v) => {
        res = v.nodeID :: res
        if (v.via != startV) findVia(v.via)
        else res = v.via :: res
      }
      case None => println("path error")
    }
    res
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
  case class Edge(linkID: String, length: Double, src: String, dst: String, isExit: Int)

  private def getStartVertex(inputDF: DataFrame): DataFrame = {
    def findStartV(positions: String, links: String) = {
      val posArraySize = positions.split("\\|").length
      val positionsUsed = Math.min(posArraySize, 80)
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
        case None      => posPairs.last
      }

      val latIncreasing = slowPositions._1.split(";")(0).toDouble < slowPositions._2.split(";")(0).toDouble
      val longIncreasing = slowPositions._1.split(";")(1).toDouble < slowPositions._2.split(";")(1).toDouble
      val slowTime = getMilliseconds(slowPositions._1.split(";"))

      val linkPairs = linksArr zip linksArr.tail
      val slowIndex = linksArr.indexWhere { link => getMilliseconds(link.split(";")) > slowTime }
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
    val withStartDF = inputDF.withColumn("startVertex", vertexCol).filter("startVertex != 'none'")
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