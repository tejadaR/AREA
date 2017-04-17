/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.controller

import rtejada.projects.AREA.model.AREAModel
import rtejada.projects.AREA.view.ResultsView
import java.util.concurrent.TimeUnit
import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.scene.shape._
import scalafx.scene.text._
import scalafx.scene.canvas._
import scalafx.scene.paint.Color._
import scalafx.scene.paint.Color
import scalafx.scene.input.MouseEvent
import scalafx.event.ActionEvent
import scalafx.scene.Node
import rtejada.projects.AREA.model.{ ForestRun, TreeNode }
import scalafx.geometry.Pos
import javafx.scene.control.ScrollPane.ScrollBarPolicy
import javafx.animation.{ Timeline, KeyFrame }
import javafx.util.Duration
import javafx.scene.control.{ Tooltip, OverrunStyle }
import scalafx.scene.image.ImageView
import scalafx.scene.Scene
import scalafx.stage.Stage
import scalafx.scene.image.Image
import org.graphframes._
import org.apache.spark.sql._
import scalafx.scene.input.KeyEvent
import scala.util.Random

class ResultsController(mlModel: => AREAModel, forestRun: ForestRun, stageW: Double, stageH: Double) {
  val model = mlModel

  def onOptSelected(airportCode: String) = {
    val airportConfig = airportCode match {
      case "KPHX" => ("phx", (-112.031132, 33.44411), (-111.9883, 33.42229))
      case "KATL" => ("atl", (-84.4502665, 33.658), (-84.4033575, 33.619))
      case "KDEN" => ("den", (-104.7311332, 39.899935), (-104.616471, 39.824725))
      case "KBWI" => ("bwi", (-76.692, 39.1894), (-76.6503, 39.16275))
    }
    val startPoint = airportConfig._2
    val endPoint = airportConfig._3
    val scPane = new ScrollPane
    scPane.prefWidth = stageW * 0.8
    scPane.prefHeight = stageH * 0.8

    val stack = new StackPane
    val airportImg = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/" + airportConfig._1 + "_map.png"), stageW * 0.8, stageH * 0.8, true, true)
    }
    val canvas = new Canvas(airportImg.getImage.getWidth, airportImg.getImage.getHeight)
    canvas.setMouseTransparent(true)
    val gc = canvas.graphicsContext2D

    stack.children.addAll(airportImg, canvas)

    val optSamples = forestRun.optExtracted.samples

    //val samplePath = mlModel.currentPath.getAs[Seq[Row]]("optimalPath")
    //samplePath.foreach(p => {
    //  println("0: " + p.getAs[String](0) + ", 1: " + p.getAs[Int](1))
    //})
    val linksDF = mlModel.loadLinks(airportCode)._1
    val graph = mlModel.loadLinks(airportCode)._2
    gc.lineWidth = 3
    /*
    linksDF.collect.foreach(row => {
      val id = row.getAs[String]("LinkID")
      gc.setStroke(Color.Blue)

      if (samplePath.exists(p => p.getAs[String](0) == id && p.getAs[Int](1) == 0)) {
        gc.setStroke(Color.OrangeRed)
        val srcLat = row.getAs[String]("srcLatitude").toDouble
        val srcLong = row.getAs[String]("srcLongitude").toDouble
        val dstLat = row.getAs[String]("dstLatitude").toDouble
        val dstLong = row.getAs[String]("dstLongitude").toDouble
        val drawSrcLat = Math.abs(srcLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawSrcLong = Math.abs(srcLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
        val drawDstLat = Math.abs(dstLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawDstLong = Math.abs(dstLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)

        gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
      } else if (samplePath.exists(p => p.getAs[String](0) == id && p.getAs[Int](1) == 1)) {
        val srcLat = row.getAs[String]("srcLatitude").toDouble
        val srcLong = row.getAs[String]("srcLongitude").toDouble
        val dstLat = row.getAs[String]("dstLatitude").toDouble
        val dstLong = row.getAs[String]("dstLongitude").toDouble
        val drawSrcLat = Math.abs(srcLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawSrcLong = Math.abs(srcLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
        val drawDstLat = Math.abs(dstLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawDstLong = Math.abs(dstLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)

        gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
      }

    })*/
    val r = Random
    val randomSample = optSamples(r.nextInt(25))
    val optPath = randomSample.optPath
    val actualPath = randomSample.actualPath
    linksDF.collect.foreach(row => {
      val runwayPattern = """[0-9]+\D?\/[0-9]+\D(\D|\.)?\D?\D?[0-9]+?\D?\_[0-9]+\D?\/[0-9]+\D(\D|\.)?\D?\D?[0-9]+?\D?""".r.unanchored
      val exitPattern = """[0-9]+\D?\/[0-9]+\D?(\.|\D)\D?\D?[0-9]+""".r.unanchored
      gc.setStroke(Color.OrangeRed)

      val srcLat = row.getAs[String]("srcLatitude").toDouble
      val srcLong = row.getAs[String]("srcLongitude").toDouble
      val dstLat = row.getAs[String]("dstLatitude").toDouble
      val dstLong = row.getAs[String]("dstLongitude").toDouble

      val drawSrcLat = Math.abs(srcLat - startPoint._2) *
        airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
      val drawSrcLong = Math.abs(srcLong - startPoint._1) *
        airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
      val drawDstLat = Math.abs(dstLat - startPoint._2) *
        airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
      val drawDstLong = Math.abs(dstLong - startPoint._1) *
        airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)

      val linkName = row.getAs[String]("LinkName")
      val linkID = row.getAs[String]("LinkID")

      gc.lineWidth = 2
      gc.setStroke(Color.Gray)
      gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)

      if (optPath.exists(l => l.linkID == linkID)) {
        gc.lineWidth = 3
        gc.setStroke(Color.Blue)
        gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
        gc.setStroke(Color.rgb(0, 50, 255))
        gc.lineWidth = 1.3
      }
      if (actualPath.exists(l => l.linkID == linkID)) {
        gc.lineWidth = 3
        gc.setStroke(Color.Red)
        gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
        gc.setStroke(Color.rgb(0, 50, 255))
        gc.lineWidth = 1.3
      }

      /*linkName match {
        case runwayPattern(_*) => {
          gc.lineWidth = 3
          gc.setStroke(Color.Gray)
          gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
        }
        case exitPattern(_*) => {
          gc.lineWidth = 3
          gc.setStroke(Color.Gray)
          gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
          gc.setStroke(Color.rgb(0, 50, 255))
          gc.lineWidth = 1.3
          val drawLat = if (drawSrcLat <= drawDstLat) {
            if (drawSrcLong <= drawDstLong) ((drawSrcLat + drawDstLat) / 2) + (Math.abs(drawSrcLat - drawDstLat) / 3)
            else ((drawSrcLat + drawDstLat) / 2) - (Math.abs(drawSrcLat - drawDstLat) / 3)
          } else {
            if (drawSrcLong <= drawDstLong) ((drawSrcLat + drawDstLat) / 2) - (Math.abs(drawSrcLat - drawDstLat) / 3)
            else ((drawSrcLat + drawDstLat) / 2) + (Math.abs(drawSrcLat - drawDstLat) / 3)
          }
          gc.strokeText(row.getAs[String]("LinkID"), (drawSrcLong + drawDstLong) / 2, drawLat)
        }
        case _ => {
          gc.lineWidth = 3
          gc.setStroke(Color.Gray)
          gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
        }
      }*/
      //gc.setStroke(Color.DarkBlue)
      //gc.lineWidth = 1
      //gc.strokeText(row.getAs[String]("LinkID"), (drawSrcLong + drawDstLong) / 2, (drawSrcLat + drawDstLat) / 2)
    })
    gc.lineWidth = 3
    graph.vertices.collect().foreach(row => {
      if (row.getAs[String]("id") == randomSample.slowV) gc.setStroke(Color.White) else gc.setStroke(Color.Yellow)
      val drawLat = Math.abs(row.apply(1).toString().toDouble - startPoint._2) * airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
      val drawLong = Math.abs(row.apply(2).toString().toDouble - startPoint._1) * airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
      gc.strokeOval(drawLong, drawLat, 5, 3)
    })

    scPane.content = stack
    val imgScene = new Scene(scPane)
    val imgStage = new Stage
    val coordsTip = new Tooltip
    Tooltip.install(airportImg, coordsTip)
    val moveEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        val xRatio = event.x / airportImg.getImage.getWidth
        val yRatio = event.y / airportImg.getImage.getHeight
        val long = BigDecimal(startPoint._1 + ((endPoint._1 - startPoint._1) * xRatio)).setScale(5, BigDecimal.RoundingMode.HALF_UP)
        val lat = BigDecimal(startPoint._2 + ((endPoint._2 - startPoint._2) * yRatio)).setScale(5, BigDecimal.RoundingMode.HALF_UP)
        coordsTip.text = "(" + long + ", " + lat + ")"
        coordsTip.show(airportImg, event.x, event.y)
        event
      }
    }
    /*
    val adjustWithKey: (KeyEvent) => KeyEvent = { event: KeyEvent =>
      {
        event.text match {
          case "y" => startPoint = (startPoint._1, startPoint._2 + 0.0001)
          case "u" => startPoint = (startPoint._1, startPoint._2 - 0.0001)
          case "i" => startPoint = (startPoint._1 - 0.0001, startPoint._2)
          case "o" => startPoint = (startPoint._1 + 0.0001, startPoint._2)
          case "h" => endPoint = (endPoint._1, endPoint._2 + 0.0001)
          case "j" => endPoint = (endPoint._1, endPoint._2 - 0.0001)
          case "k" => endPoint = (endPoint._1 - 0.0001, endPoint._2)
          case "l" => endPoint = (endPoint._1 + 0.0001, endPoint._2)
          case _   =>
        }
        println("start: " + "(" + startPoint._1 + "," + startPoint._2 + ")")
        println("end: " + "(" + endPoint._1 + "," + endPoint._2 + ")")
        println()
        gc.clearRect(0, 0, gc.getCanvas.getWidth, gc.getCanvas.getHeight)
        linksDF.collect.foreach(row => {
          val runwayPattern = """[0-9]+\D?\/[0-9]+\D?(\.|\D)\D?\D?[0-9]+\_[0-9]+\D?\/[0-9]+\D?(\.|\D)\D?\D?[0-9]+""".r.unanchored
          val exitFirstPattern = """[0-9]+\D?\/[0-9]+\D?(\.|\D)\D?\D?[0-9]+""".r.unanchored
          gc.setStroke(Color.OrangeRed)

          val srcLat = row.getAs[String]("srcLatitude").toDouble
          val srcLong = row.getAs[String]("srcLongitude").toDouble
          val dstLat = row.getAs[String]("dstLatitude").toDouble
          val dstLong = row.getAs[String]("dstLongitude").toDouble

          val drawSrcLat = Math.abs(srcLat - startPoint._2) *
            airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
          val drawSrcLong = Math.abs(srcLong - startPoint._1) *
            airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
          val drawDstLat = Math.abs(dstLat - startPoint._2) *
            airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
          val drawDstLong = Math.abs(dstLong - startPoint._1) *
            airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)

          val linkName = row.getAs[String]("LinkName")

          gc.lineWidth = 3
          linkName match {
            case runwayPattern(_*)    => gc.setStroke(Color.OrangeRed)
            case exitFirstPattern(_*) => gc.setStroke(Color.Blue)
            case _                    => gc.setStroke(Color.OrangeRed)
          }
          gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
        })
        gc.lineWidth = 2
        gc.setStroke(Color.Yellow)

        graph.vertices.collect().foreach(row => {
          val drawLat = Math.abs(row.apply(1).toString().toDouble - startPoint._2) * airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
          val drawLong = Math.abs(row.apply(2).toString().toDouble - startPoint._1) * airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
          gc.strokeOval(drawLong, drawLat, 5, 3)
        })

        event
      }
    }*/
    airportImg.setOnMouseMoved(moveEvent)
    //imgScene.setOnKeyPressed(keyE)
    imgStage.scene = imgScene
    imgStage.onCloseRequest = handle { imgStage.close() }
    imgStage.showAndWait
  }

  /** Gets a string with minutes and seconds from a millisecond input */
  def getMinSecStr(millis: Int): String = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(millis.toLong)
    val extraSeconds = TimeUnit.MILLISECONDS.toSeconds(millis.toLong) - (minutes * 60)
    minutes + "m " + extraSeconds + "s"
  }

  /** Generates a tree pane and its importances label for the view to show */
  def onSelectTree(tree: Int): (ScrollPane, Label) = {
    val newPane = genTreePane(tree)
    val newImportancesLabel = genTreeImportancesLabel(tree)
    (newPane, newImportancesLabel)
  }

  /** Generates tree grid, made visible to view so that default tree is loaded*/
  def genTreePane(treeIndex: Int): ScrollPane = {

    def getCellsX(depth: Int) = (Math.pow(2, depth) + 1).toInt
    val treeStack = new StackPane
    treeStack.setAlignment(Pos.TopLeft)

    val maxDepth = forestRun.getForestMaxDepth
    val treeCellsX = getCellsX(maxDepth)
    val treeCellsY = maxDepth + 1
    val nodeRadius = 7
    val paddingH = 2
    val paddingV = 15

    val treeCanvas = new Canvas(treeCellsX * (paddingH + nodeRadius * 2), treeCellsY * (paddingV + nodeRadius * 2))
    treeCanvas.setMouseTransparent(true)
    val gc = treeCanvas.graphicsContext2D
    gc.setStroke(Color.Black)

    val grid = new GridPane()
    grid.setHgap(paddingH)
    grid.setVgap(paddingV)
    for (i <- 1 to treeCellsX)
      grid.getColumnConstraints.add(new ColumnConstraints(nodeRadius * 2))

    val rootNode = forestRun.forest(treeIndex)
    val rootPosX = (treeCellsX - 1) / 2
    rootNode.setGridPosX(rootPosX)
    val rootShape = getNodeShape(rootNode, nodeRadius)
    grid.add(rootShape, rootPosX, rootNode.getDepth)

    fillTreeGrid(rootNode, grid, gc, maxDepth, nodeRadius, paddingH, paddingV)
    treeStack.children = List(grid, treeCanvas)

    val scrollPane = new ScrollPane
    scrollPane.setHbarPolicy(ScrollBarPolicy.ALWAYS)
    scrollPane.setVbarPolicy(ScrollBarPolicy.NEVER)
    scrollPane.content = treeStack

    scrollPane
  }

  /** Recursively draws and links nodes on the grid */
  private def fillTreeGrid(parent: TreeNode, grid: GridPane, gc: GraphicsContext, maxDepth: Int, nodeRadius: Int,
                           paddingH: Int, paddingV: Int): Unit = {
    val leftChild = parent.getChild(true).get
    val rightChild = parent.getChild(false).get

    val leftChildX = (if (maxDepth - parent.getDepth < 2)
      if (parent.getSide == "left") parent.getGridPosX - 1 else parent.getGridPosX
    else
      parent.getGridPosX - Math.pow(2, maxDepth - parent.getDepth - 2)).toInt
    val rightChildX = (if (maxDepth - parent.getDepth < 2)
      if (parent.getSide == "left") parent.getGridPosX else parent.getGridPosX + 1
    else
      parent.getGridPosX + Math.pow(2, maxDepth - parent.getDepth - 2)).toInt

    leftChild.setGridPosX(leftChildX)
    rightChild.setGridPosX(rightChildX)

    val bothPosY = leftChild.getDepth

    val leftShape = getNodeShape(leftChild, nodeRadius)
    val rightShape = getNodeShape(rightChild, nodeRadius)

    grid.add(leftShape, leftChildX, bothPosY)
    grid.add(rightShape, rightChildX, bothPosY)

    val startPointX = parent.getGridPosX * (nodeRadius * 2 + paddingH) + nodeRadius
    val startPointY = (nodeRadius * 2) + parent.getDepth * (nodeRadius * 2 + paddingV)

    gc.strokeLine(startPointX, startPointY, leftChildX * (nodeRadius * 2 + paddingH) + nodeRadius,
      bothPosY * (nodeRadius * 2 + paddingV))
    gc.strokeLine(startPointX, startPointY, rightChildX * (nodeRadius * 2 + paddingH) + nodeRadius,
      bothPosY * (nodeRadius * 2 + paddingV))

    if (leftChild.getChild(true) != None)
      fillTreeGrid(leftChild, grid, gc, maxDepth, nodeRadius, paddingH, paddingV)
    if (rightChild.getChild(true) != None)
      fillTreeGrid(rightChild, grid, gc, maxDepth, nodeRadius, paddingH, paddingV)
  }

  /** Generates node image */
  private def getNodeShape(node: TreeNode, nodeRadius: Int): Circle = {
    val shape = new Circle {
      radius = (when(hover) choose (nodeRadius * 1.5) otherwise nodeRadius).toDouble
      fill = if (node.getDepth == 0) Color.Gold
      else if (node.getChildren.isEmpty) Color.DarkBlue
      else Color.LightSeaGreen
    }
    val conditionTip = new Tooltip
    conditionTip.wrapText = true
    conditionTip.setMaxWidth(stageW * 0.6)
    adjustTooltipDelay(conditionTip)
    Tooltip.install(shape, conditionTip)
    val enterEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        shape.fill = Color.Coral
        conditionTip.text = genConditionStr(node)
        event
      }
    }
    val exitEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        shape.fill = if (node.getDepth == 0) Color.Gold
        else if (node.getChildren.isEmpty) Color.DarkBlue
        else Color.LightSeaGreen
        event
      }
    }
    shape.setOnMouseEntered(enterEvent)
    shape.setOnMouseExited(exitEvent)
    shape
  }

  /** Creates a generic label with a specified size */
  private def genConditionStr(node: TreeNode): String = {
    if (node.getChild(true) == None) "Predict Exit " + forestRun.getExitLabel(node.getPrediction)
    else {
      val rawCondition = node.getCondition
      val featName = formatFeature(forestRun.getFeatureName(node.getFeatureIndex))
      val featCatArr = forestRun.getFeatureCategories(node.getFeatureIndex)
      featCatArr match {
        case None => "Is " + featName + " " + rawCondition + "?"
        case Some(featureCategories) => "Is " + featName + " in (" + {
          val start = rawCondition.indexOf("{") + 1
          val end = rawCondition.indexOf("}")
          val rawCatArr = rawCondition.substring(start, end).split(",").map(_.toDouble.toInt)
          rawCatArr.map(featureCategories(_)).mkString(", ") + ")" + "?"
        }
      }
    }
  }

  /**Generates importances label for a tree */
  def genTreeImportancesLabel(treeIndex: Int): Label = {
    val numFeatures = forestRun.forestImportances.length
    val treeImportances = forestRun.treeImportancesList

    val importancesText = (for (i <- 0 until numFeatures) yield {
      val featureName = formatFeature(forestRun.getFeatureName(i))
      val roundedImportance = "%.2f".format(treeImportances(treeIndex)(i) * 100).toDouble
      "\t" + featureName + ": " + roundedImportance + " %" + System.lineSeparator()
    }).mkString

    new Label(importancesText)
  }

  /** Decreases the activation delay of the given tooltip */
  private def adjustTooltipDelay(tooltip: javafx.scene.control.Tooltip) = {
    val fieldBehavior = tooltip.getClass.getDeclaredField("BEHAVIOR")
    fieldBehavior.setAccessible(true)
    val objectBehavior = fieldBehavior.get(tooltip)

    val fieldTimer = objectBehavior.getClass.getDeclaredField("activationTimer")
    fieldTimer.setAccessible(true)
    val objTimer = fieldTimer.get(objectBehavior).asInstanceOf[Timeline]

    objTimer.getKeyFrames.clear
    objTimer.getKeyFrames.add(new KeyFrame(new Duration(0)))
  }

  /** Formats features for ease of use*/
  def formatFeature(in: String): String = in match {
    case "runway"        => "Runway"
    case "depAirport"    => "Origin Airport"
    case "aircraftType"  => "Aircraft Type"
    case "arrTerminal"   => "Arrival Terminal"
    case "arrGate"       => "Arrival Gate"
    case "touchdownLat"  => "Touchdown Latitude"
    case "touchdownLong" => "Touchdown Longitude"
    case "hour"          => "Hour"
    case "day"           => "Day of Week"
    case "decel"         => "Deceleration(m/s\u00B2)"
    case "carrier"       => "Airline"
    case "traffic"       => "Traffic"
  }

}