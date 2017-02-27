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

class ResultsView(controller: => ResultsController, forestRun: ForestRun, stageWidth: Double, stageHeight: Double) {
  val tab = new Tab
  val viewBox = new VBox
  tab.content = viewBox
  tab.text = forestRun.getAirportCode
  viewBox.styleClass.add("container")

  val infoBox = genInfoBox(stageWidth, stageHeight * 0.45)
  val forest = new ForestBox(forestRun, stageWidth, stageHeight * 0.55)
  val completeBox = forest.completeBox
  viewBox.children.addAll(infoBox, completeBox)

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

    val detailsBox = new VBox
    val lblAcc = new Label("Accuracy: " + forestRun.getAccuracy + "%")
    val lblTrainRecords = new Label("Training Records: " + forestRun.getTrainCount)
    val lblTestRecords = new Label("Testing Records: " + forestRun.getTestCount)
    val lblDuration = new Label("Run Duration: " + controller.getMinSecStr(forestRun.getDuration))
    val lblDate = new Label(forestRun.getDate)
    detailsBox.children.addAll(lblAcc, lblTrainRecords, lblTestRecords, lblDuration, lblDate)

    val featurePie = new PieChart {
      data = forestRun.forestImportances.zipWithIndex.map {
        case (importance, index) =>
          PieChart.Data(forestRun.getFeatureName(index), importance)
      }
      clockwise = false
      prefHeight = h
    }

    val lblImportancesLabelTitle = new Label("Forest Feature Importances: " + System.lineSeparator())
    val lblImportancesLabel = new Label(forestRun.forestImportances.zipWithIndex.map {
      case (importance, index) => "\t" + forestRun.getFeatureName(index) + ": " +
        "%.2f".format(importance * 100).toDouble + " %" + System.lineSeparator()
    }.mkString)
    val forestImportancesBox = new VBox()
    forestImportancesBox.children.addAll(lblImportancesLabelTitle, lblImportancesLabel)
    val forestImportancesPane = new AnchorPane()
    forestImportancesPane.children.addAll(forestImportancesBox)

    bodyBox.children.addAll(detailsBox, featurePie, forestImportancesPane)

  }

}