/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

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

  val infoBox = genInfoBox(stageWidth, stageHeight * 0.65)
  val forestBox = genViewerBox(stageWidth, stageHeight * 0.35)
  viewBox.children.addAll(infoBox, forestBox)

  /** Generates information summary module*/
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
    val lblAcc = new Label("Accuracy: " +
      BigDecimal(forestRun.getAccuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP) + "%")
    val lblTrainRecords = new Label("Training Records: " + forestRun.getTrainCount)
    val lblTestRecords = new Label("Testing Records: " + forestRun.getTestCount)
    val lblDuration = new Label("Run Duration: " + controller.getMinSecStr(forestRun.getDuration))
    val lblDate = new Label(forestRun.getDate)
    val lblOptRot = new Label("Optimal ROT: " +
      BigDecimal(forestRun.optExtracted.optROT).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    val lblactualRot = new Label("Actual ROT: " +
      BigDecimal(forestRun.optExtracted.actualROT).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    val lblOptTT = new Label("Optimal TT: " +
      BigDecimal(forestRun.optExtracted.optTT).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    val lblActualTT = new Label("Actual TT: " +
      BigDecimal(forestRun.optExtracted.actualTT).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    val optButton = new Button("Optimization") {
      onAction = (ae: ActionEvent) => {
        controller.onOptSelected(forestRun.getAirportCode)
      }
    }
    detailsBox.children.addAll(lblAcc, lblTrainRecords, lblTestRecords, lblDuration, lblDate,
      lblOptRot, lblactualRot, lblOptTT, lblActualTT, optButton)
    detailsBox.spacing = h * 0.01
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

  /** Generates tree-viewer module*/
  private def genViewerBox(w: Double, h: Double) = new VBox with TitledModuleH {

    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.08
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.92

    val titleLabel = new Label("Tree Viewer")
    val numTreesLabel = new Label(forestRun.getForestNumTrees + " trees")
    val maxDepthLabel = new Label("Max depth: " + forestRun.getForestMaxDepth)

    val gridPane = controller.genTreePane(0) // default first tree
    gridPane.prefHeight = bodyPane.getPrefHeight * 0.9
    gridPane.hvalue = 0.5

    val treeBranchLabel = new Label("Left branch = YES,  Right branch = NO")
    treeBranchLabel.styleClass.add("importantTip")

    val viewerBox = new VBox
    viewerBox.children = List(treeBranchLabel, gridPane)
    viewerBox.setPrefWidth(bodyPane.getPrefWidth * 0.95)
    val numTrees = forestRun.getForestNumTrees
    val treeSelector = new ComboBox[Int] {
      promptText = "Select Tree..."
      editable = false
      items = ObservableBuffer(1 to numTrees)
      selectionModel().selectFirst()
      onAction = (event: ActionEvent) => {
        val paneAndLabel = controller.onSelectTree(selectionModel().getSelectedItem - 1)
        val newPane = paneAndLabel._1
        val newLabel = paneAndLabel._2
        newPane.prefHeight = bodyPane.getPrefHeight
        newPane.hvalue = 0.5
        viewerBox.children.remove(viewerBox.children.last)
        viewerBox.children.add(newPane)
      }
    }
    treeSelector.styleClass.add("selector")

    val headerBox = new HBox
    headerBox.spacing = stageWidth * 0.8 * 0.1
    headerBox.children.addAll(titleLabel, treeSelector, numTreesLabel, maxDepthLabel)
    headerBox.setPrefHeight(bodyPane.getPrefHeight * 0.1)
    headerPane.children.add(headerBox)
    AnchorPane.setAnchors(headerBox, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)
    AnchorPane.setTopAnchor(bodyBox, bodyPane.getPrefHeight * 0.008d)
    AnchorPane.setLeftAnchor(bodyBox, bodyPane.getPrefWidth * 0.008d)
    bodyBox.children.addAll(viewerBox)
  }

}