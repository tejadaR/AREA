package rtejada.projects.AREA.view

import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.scene.Parent
import scalafx.scene.Node
import scalafx.collections.ObservableBuffer
import scalafx.geometry.{ Pos, Insets }
import scalafx.event.ActionEvent
import scalafx.scene.image.{ Image, ImageView }
import rtejada.projects.AREA.controller.ResultsController
import rtejada.projects.AREA.model.ForestRun
import scalafx.scene.chart.PieChart
import scalafx.geometry.Side
import scalafx.scene.text.TextAlignment

class ResultsView(controller: => ResultsController, forestRun: ForestRun, stageWidth: Double, stageHeight: Double) {
  val tab = new Tab
  val viewBox = new VBox
  tab.content = viewBox
  tab.text = forestRun.getAirportCode
  viewBox.styleClass.add("container")

  val infoBox = genInfoBox(stageWidth, stageHeight * 0.45)
  val forestBox = genViewerBox(stageWidth, stageHeight * 0.55)
  viewBox.children.addAll(infoBox, forestBox)

  private def genInfoBox(w: Double, h: Double) = new VBox with TitledModuleH {
    val airportTitle = new Label(forestRun.getAirportCode + " AIRPORT")
    val runwaysTitle = new Label("# Runway configurations: " + forestRun.getNumRunways)
    val exitsTitle = new Label("# Exits: " + forestRun.getNumExits)
    val airportIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/airportinv.png"), h * 0.07, h * 0.07, true, true)
    }
    val runwaysIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/runwaysinv.png"), h * 0.07, h * 0.07, true, true)
    }
    val exitsIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/exitsinv.png"), h * 0.07, h * 0.07, true, true)
    }
    val airportBox = new HBox(spacing = w * 0.02)
    airportBox.children.addAll(airportIcon, airportTitle)
    val runwaysBox = new HBox(spacing = w * 0.02)
    runwaysBox.children.addAll(runwaysIcon, runwaysTitle)
    val exitsBox = new HBox(spacing = w * 0.02)
    exitsBox.children.addAll(exitsIcon, exitsTitle)

    val headerBox = new HBox(spacing = w * 0.1)
    headerBox.children.addAll(airportBox, runwaysBox, exitsBox)

    headerPane.children.add(headerBox)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.08
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.92
    AnchorPane.setAnchors(headerBox, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)

    val detailsPane = new AnchorPane
    val detailsBox = new VBox
    val lblAcc = new Label("Accuracy: " + BigDecimal(forestRun.getAccuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP) + "%")
    val lblTrainRecords = new Label("Training Records: " + forestRun.getTrainCount)
    val lblTestRecords = new Label("Testing Records: " + forestRun.getTestCount)
    val lblDuration = new Label("Run Duration: " + controller.getMinSecStr(forestRun.getDuration))
    val lblDate = new Label(forestRun.getDate)
    detailsBox.children.addAll(lblAcc, lblTrainRecords, lblTestRecords, lblDuration, lblDate)
    detailsBox.spacing = h * 0.1
    detailsBox.prefWidth = w * 0.3
    detailsBox.styleClass.add("summary")
    detailsPane.children.add(detailsBox)

    val featurePie = new PieChart {
      data = forestRun.forestImportances.zipWithIndex.map {
        case (importance, index) =>
          PieChart.Data(controller.formatFeature(forestRun.getFeatureName(index)), importance)
      }
      title = "Forest Feature Importances"
      legendSide = Side.Bottom
      labelLineLength = 20
      startAngle = 180
      clockwise = false
      prefHeight = h
      prefWidth = w * 0.4
      animated = true
    }

    val lblImportancesLabel = new Label(forestRun.forestImportances.zipWithIndex.map {
      case (importance, index) => "\t" + controller.formatFeature(forestRun.getFeatureName(index)) + ": " +
        "%.2f".format(importance * 100).toDouble + " %" + System.lineSeparator() + System.lineSeparator()
    }.mkString)
    val forestImportancesPane = new AnchorPane()
    forestImportancesPane.children.addAll(lblImportancesLabel)
    forestImportancesPane.prefWidth = w * 0.3

    AnchorPane.setAnchors(bodyBox, w * 0.01, 0, 0, w * 0.01)
    bodyBox.children.addAll(detailsBox, featurePie, forestImportancesPane)
  }

  private def genViewerBox(w: Double, h: Double) = new VBox with TitledModuleH {

    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.08
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.92

    val titleLabel = new Label("Tree Viewer")
    val numTreesLabel = new Label(forestRun.getForestNumTrees + " trees")
    val maxDepthLabel = new Label("Max depth: " + forestRun.getForestMaxDepth)
    val conditionLabel = new Label("Hover over nodes to see conditions or predictions (left branch= TRUE, right branch= FALSE)")
    conditionLabel.setPrefHeight(bodyPane.getPrefHeight * 0.25)
    conditionLabel.wrapText = true
    conditionLabel.setContentDisplay(ContentDisplay.Top)
    conditionLabel.alignmentInParent = Pos.TopLeft

    val gridPane = controller.genTreePane(0, conditionLabel) // default first tree
    gridPane.setPrefHeight(bodyPane.getPrefHeight * 0.65)
    gridPane.hvalue = 0.5

    val treeImpTitle = new Label("Tree Feature Importances")
    val treeImportancesLabel = controller.genTreeImportancesLabel(0)
    val treeImpBox = new VBox {
      children = List(treeImpTitle, treeImportancesLabel)
    }
    treeImpBox.setPrefWidth(bodyPane.getPrefWidth * 0.2)

    val viewerBox = new VBox
    viewerBox.children = List(gridPane, conditionLabel)
    viewerBox.setPrefWidth(bodyPane.getPrefWidth * 0.8)
    val numTrees = forestRun.getForestNumTrees
    val treeSelector = new ComboBox[Int] {
      promptText = "Select Tree..."
      editable = false
      items = ObservableBuffer(1 to numTrees)
      selectionModel().selectFirst()
      onAction = (event: ActionEvent) => {
        val paneAndLabel = controller.onSelectTree(selectionModel().getSelectedItem - 1, conditionLabel)
        val newPane = paneAndLabel._1
        val newLabel = paneAndLabel._2
        newPane.setPrefHeight(bodyPane.getPrefHeight * 0.65)
        newPane.hvalue = 0.5
        viewerBox.children.remove(0)
        viewerBox.children.insert(0, newPane)
        treeImpBox.children.remove(treeImpBox.children.last)
        treeImpBox.children.add(newLabel)
      }
    }
    val headerBox = new HBox
    headerBox.spacing = stageWidth * 0.8 * 0.1
    headerBox.children.addAll(titleLabel, treeSelector, numTreesLabel, maxDepthLabel)
    headerBox.setPrefHeight(bodyPane.getPrefHeight * 0.1)
    headerPane.children.add(headerBox)
    AnchorPane.setAnchors(headerBox, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)
    bodyBox.children.addAll(viewerBox, treeImpBox)
  }

}