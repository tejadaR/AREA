package rtejada.projects.AREA.controller

import rtejada.projects.AREA.model.MLModel
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
import rtejada.projects.AREA.model.ForestRun
import rtejada.projects.AREA.model.TreeNode
import scalafx.geometry.Pos
import scalafx.scene.control.ScrollPane.ScrollBarPolicy

class ResultsController(mlModel: => MLModel, forestRun: ForestRun) {
  val model = mlModel

  /** Gets a string with minutes and seconds from a millisecond input */
  def getMinSecStr(millis: Int): String = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(millis.toLong)
    val extraSeconds = TimeUnit.MILLISECONDS.toSeconds(millis.toLong) - (minutes * 60)
    minutes + "m " + extraSeconds + "s"
  }

  def onSelectTree(tree: Int, conditionLabel: Label): (ScrollPane, Label) = {
    val newPane = genTreePane(tree, conditionLabel)
    val newImportancesLabel = genTreeImportancesLabel(tree)

    (newPane, newImportancesLabel)
  }

  /** Generates tree grid */
  def genTreePane(treeIndex: Int, conditionLabel: Label): ScrollPane = {

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
    val rootShape = getNodeShape(rootNode, nodeRadius, conditionLabel)
    grid.add(rootShape, rootPosX, rootNode.getDepth)

    fillTreeGrid(rootNode, grid, gc, maxDepth, nodeRadius, paddingH, paddingV, conditionLabel)
    treeStack.children = List(grid, treeCanvas)

    val scrollPane = new ScrollPane
    scrollPane.setHbarPolicy(ScrollBarPolicy.Always)
    scrollPane.setVbarPolicy(ScrollBarPolicy.Never)

    scrollPane.content = treeStack

    scrollPane
  }
  /** Recursively draws and links nodes on the grid */
  private def fillTreeGrid(parent: TreeNode, grid: GridPane, gc: GraphicsContext, maxDepth: Int, nodeRadius: Int,
                           paddingH: Int, paddingV: Int, conditionLabel: Label): Unit = {
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

    val leftShape = getNodeShape(leftChild, nodeRadius, conditionLabel)
    val rightShape = getNodeShape(rightChild, nodeRadius, conditionLabel)

    grid.add(leftShape, leftChildX, bothPosY)
    grid.add(rightShape, rightChildX, bothPosY)

    val startPointX = parent.getGridPosX * (nodeRadius * 2 + paddingH) + nodeRadius
    val startPointY = (nodeRadius * 2) + parent.getDepth * (nodeRadius * 2 + paddingV)

    gc.strokeLine(startPointX, startPointY, leftChildX * (nodeRadius * 2 + paddingH) + nodeRadius,
      bothPosY * (nodeRadius * 2 + paddingV))
    gc.strokeLine(startPointX, startPointY, rightChildX * (nodeRadius * 2 + paddingH) + nodeRadius,
      bothPosY * (nodeRadius * 2 + paddingV))

    if (leftChild.getChild(true) != None)
      fillTreeGrid(leftChild, grid, gc, maxDepth, nodeRadius, paddingH, paddingV, conditionLabel)
    if (rightChild.getChild(true) != None)
      fillTreeGrid(rightChild, grid, gc, maxDepth, nodeRadius, paddingH, paddingV, conditionLabel)
  }

  /** Generates node image */
  private def getNodeShape(node: TreeNode, nodeRadius: Int, conditionLabel: Label): Circle = {
    val shape = new Circle {
      radius = (when(hover) choose (nodeRadius * 1.5) otherwise nodeRadius).toDouble
      fill = if (node.getDepth == 0) Color.Gold
      else if (node.getChildren.isEmpty) Color.DarkBlue
      else Color.MediumTurquoise
    }
    val enterEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        shape.fill = Color.Coral
        conditionLabel.text = genConditionStr(node)
        event
      }
    }
    val exitEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        shape.fill = if (node.getDepth == 0) Color.Gold
        else if (node.getChildren.isEmpty) Color.DarkBlue
        else Color.MediumTurquoise
        conditionLabel.text = " "
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
      val featName = forestRun.getFeatureName(node.getFeatureIndex)
      val featCatArr = forestRun.getFeatureCategories(node.getFeatureIndex)
      featCatArr match {
        case None => featName + " " + rawCondition
        case Some(featureCategories) => featName + " is in (" + {
          val start = rawCondition.indexOf("{") + 1
          val end = rawCondition.indexOf("}")
          val rawCatArr = rawCondition.substring(start, end).split(",").map(_.toDouble.toInt)
          rawCatArr.map(featureCategories(_)).mkString(", ") + ")"
        }
      }
    }
  }

  def genTreeImportancesLabel(treeIndex: Int): Label = {
    val numFeatures = forestRun.forestImportances.length
    val treeImportances = forestRun.treeImportancesList

    val importancesText = (for (i <- 0 until numFeatures) yield {
      val featureName = forestRun.getFeatureName(i)
      val roundedImportance = "%.2f".format(treeImportances(treeIndex)(i) * 100).toDouble
      "\t" + featureName + ": " + roundedImportance + " %" + System.lineSeparator()
    }).mkString

    new Label(importancesText)
  }

}