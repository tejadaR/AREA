package rtejada.projects.AREA.view

import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.scene.shape._
import scalafx.scene.text._
import scalafx.scene.canvas._
import scalafx.scene.paint.Color._
import scalafx.geometry.Pos
import scalafx.collections.ObservableBuffer
import scalafx.scene.control.ScrollPane.ScrollBarPolicy
import scalafx.scene.chart.PieChart
import scalafx.event.ActionEvent
import java.util.concurrent.TimeUnit
import java.util.Calendar
import scalafx.scene.paint.Color
import scalafx.scene.input.MouseEvent
import scalafx.scene.Node
import rtejada.projects.AREA.model.ForestRun
import rtejada.projects.AREA.model.TreeNode

class ForestBox(forestRun: ForestRun, stageWidth: Double, stageHeight: Double) {

  val backgroundColor = "-fx-background-color: rgb(255,255,255);"
  val conditionLabel = genLabel("Hover over a node to see its condition", 12, Color.Black, "left")
  val treeImpTitle = genLabel("Tree Feature Importances", 16, Color.White, "right")
  val treeImportancesLabel = genTreeImportancesLabel(0)
  val viewerBoxHeight = stageHeight

  val treeImpBox = new VBox {
    //spacing = 2
    //alignment = Pos.Center
    //alignmentInParent = Pos.TopCenter
    //hgrow = Priority.Always
    children = List(treeImpTitle, treeImportancesLabel)
  }
  setSubTitleStyle(treeImpTitle)
  treeImpTitle.setPrefWidth(stageWidth * 0.2)
  treeImpTitle.setPrefHeight(viewerBoxHeight * 0.1)

  val completeBox = new HBox {
    spacing = 2
    //alignment = Pos.Center
    //alignmentInParent = Pos.TopCenter
    hgrow = Priority.Always
  }
  completeBox.setStyle(backgroundColor)

  val viewerBox = getViewerBox
  treeImpBox.setPrefWidth(stageWidth * 0.2)
  viewerBox.setPrefWidth(stageWidth * 0.8)

  completeBox.children.addAll(viewerBox, treeImpBox)

  private def getViewerBox(): VBox = {
    val gridPane = genGridPane(0) // default first tree
    val viewerBox = new VBox
    val selectorBox = getSelectorBox(viewerBox, completeBox)

    gridPane.setPrefHeight(viewerBoxHeight * 0.65)
    gridPane.hvalue = 0.5
    conditionLabel.setPrefHeight(viewerBoxHeight * 0.25)

    setSubTitleStyle(selectorBox)
    selectorBox.setPrefHeight(viewerBoxHeight * 0.1)

    viewerBox.children = List(selectorBox, gridPane, conditionLabel)

    viewerBox
  }

  private def getSelectorBox(viewerBox: VBox, completeBox: HBox): HBox = {
    val numTrees = forestRun.getForestNumTrees
    val treeSelector = new ComboBox[Int] {
      maxWidth = 200
      promptText = "Select Tree..."
      editable = false
      items = ObservableBuffer(1 to numTrees)
    }

    treeSelector.onAction = (event: ActionEvent) => {
      val selectedTree = treeSelector.getSelectionModel.getSelectedItem - 1
      val newGridPane = genGridPane(selectedTree)
      newGridPane.setStyle(backgroundColor)
      newGridPane.setPrefHeight(viewerBoxHeight * 0.65)
      newGridPane.hvalue = 0.5
      val newImportancesLabel = genTreeImportancesLabel(selectedTree)
      viewerBox.children.remove(viewerBox.children.get(viewerBox.children.length - 2))
      viewerBox.children.insert(1, newGridPane)
      treeImpBox.children.remove(treeImpBox.children.last)
      treeImpBox.children.add(newImportancesLabel)
    }

    val numTreesLabel = genLabel(numTrees + " trees", 16, Color.White, "right")
    val maxDepthLabel = genLabel("Max depth: " + forestRun.getForestMaxDepth, 16, Color.White, "right")
    maxDepthLabel.setTextAlignment(TextAlignment.Right)
    val selectorBox = new HBox
    selectorBox.spacing = stageWidth * 0.8 * 0.1
    selectorBox.children.addAll(treeSelector, numTreesLabel, maxDepthLabel)
    selectorBox
  }

  /** Creates a vertical box with a title label, the grid, and the tree's importances */
  private def genGridPane(treeIndex: Int): ScrollPane = {
    val numFeatures = forestRun.forestImportances.length
    val treeImportances = forestRun.treeImportancesList
    val treePane = genTreePane(treeIndex)

    treePane
  }

  /** Generates tree grid */
  private def genTreePane(treeIndex: Int): ScrollPane = {
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
    treeStack.setStyle(backgroundColor)

    val scrollPane = new ScrollPane
    scrollPane.setHbarPolicy(ScrollBarPolicy.Always)
    scrollPane.setVbarPolicy(ScrollBarPolicy.Never)

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

  /**
   * Gets cell number necessary for a single tree's grid width
   */
  private def getCellsX(depth: Int) = (Math.pow(2, depth) + 1).toInt

  /** Generates node image */
  private def getNodeShape(node: TreeNode, nodeRadius: Int): Circle = {
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

  private def genTreeImportancesLabel(treeIndex: Int): Label = {
    val numFeatures = forestRun.forestImportances.length
    val treeImportances = forestRun.treeImportancesList

    val importancesText = (for (i <- 0 until numFeatures) yield {
      val featureName = forestRun.getFeatureName(i)
      val roundedImportance = "%.2f".format(treeImportances(treeIndex)(i) * 100).toDouble
      "\t" + featureName + ": " + roundedImportance + " %" + System.lineSeparator()
    }).mkString

    genLabel(importancesText, 14, Color.Black, "right")
  }

  /** Creates a generic label with a specified size */
  private def genLabel(txt: String, size: Int, color: Color, textAlign: String) = new Label {
    text = txt
    textFill = color
    wrapText = true
    font = Font.font("Arial", FontWeight.Bold, FontPosture.Regular, size)
    textAlignment = textAlign match {
      case "left"    => TextAlignment.Left
      case "center"  => TextAlignment.Center
      case "right"   => TextAlignment.Right
      case "justify" => TextAlignment.Justify
    }
  }

  /** Gets a string with minutes and seconds from a millisecond input */
  private def getMinSecStr(millis: Int): String = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(millis.toLong)
    val extraSeconds = TimeUnit.MILLISECONDS.toSeconds(millis.toLong) - (minutes * 60)
    minutes + "m " + extraSeconds + "s"
  }

  private def setSubTitleStyle(node: Node) = {
    node.setStyle("-fx-effect: dropshadow(one-pass-box , white, 0, 1 , 0, 1);" +
      "-fx-background-color: linear-gradient(to bottom, rgb(77,120, 215) 0%, rgb(37,80,175) 100%); " +
      "-fx-padding: 2px;")
  }

}