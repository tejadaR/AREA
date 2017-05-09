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
import rtejada.projects.AREA.controller.DashboardController
import rtejada.projects.AREA.model.ForestRun
import scalafx.geometry.Side
import scalafx.scene.text.TextAlignment
import scalafx.scene.control.ScrollPane.ScrollBarPolicy
import javafx.scene.text.Text
import javafx.collections.ObservableList
import javafx.collections.FXCollections
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scalafx.scene.chart._

/** Dashboard View class, contains tiers filled with display modules */
class DashboardView(controller: => DashboardController, forestRun: ForestRun, stageWidth: Double, stageHeight: Double) {
  val viewBox = new VBox
  viewBox.padding = Insets(stageHeight * 0.02, stageWidth * 0.01, stageHeight * 0.02, stageWidth * 0.01)
  viewBox.spacing = stageHeight * 0.025

  val scPane = new ScrollPane
  scPane.setHbarPolicy(ScrollBarPolicy.Never)
  scPane.setVbarPolicy(ScrollBarPolicy.Always)
  scPane.content = viewBox

  val tab = new Tab
  tab.content = scPane
  tab.text = forestRun.getAirportCode + " View"
  viewBox.styleClass.add("container")

  val tierWidth = stageWidth
  val tierHeight = stageHeight * 0.24

  val tierOne = genTier(tierWidth, tierHeight)
  val tierTwo = genTier(tierWidth, tierHeight)
  val tierThree = new HBox(spacing = tierWidth * 0.05)
  tierThree.prefWidth = tierWidth
  tierThree.prefHeight = tierWidth * 0.6
  val tierFour = genTier(tierWidth, tierHeight)

  val fullTierOne = fillTierOne(tierOne, tierWidth, tierHeight)
  val fullTierTwo = fillTierTwo(tierTwo, tierWidth, tierHeight)
  val fullTierThree = fillTierThree(tierThree, tierWidth, tierHeight)
  val fullTierFour = fillTierFour(tierFour, tierWidth, tierHeight)

  viewBox.children.addAll(fullTierOne, fullTierTwo, fullTierThree, fullTierFour)

  /** Generates a single tier with the specified width and height */
  private def genTier(w: Double, h: Double): HBox = {
    val tierBox = new HBox(spacing = w * 0.05)
    tierBox.minWidth = w
    tierBox.minHeight = h
    tierBox.prefWidth = w
    tierBox.prefHeight = h
    tierBox
  }

  /** Fills tier with relevant cells. Width and height provided due to HBox not having set sizes before being added to stage */
  private def fillTierOne(tier: HBox, w: Double, h: Double): HBox = {
    val airportInfoCell = genAirportInfoCell(w * 0.23D, h)
    val runInfoCell = genRunInfoCell(w * 0.3D, h)
    val featurePieCell = genFeaturePieCell(w * 0.32D, h)
    airportInfoCell.styleClass.add("darkOne")
    runInfoCell.styleClass.add("brightFour")
    featurePieCell.styleClass.add("brightOne")
    tier.children.addAll(airportInfoCell, runInfoCell, featurePieCell)
    tier
  }

  /** Creates Airport Information display module */
  private def genAirportInfoCell(w: Double, h: Double) = {
    val airportTitle = new Label(forestRun.getAirportCode + " Airport")
    val runwaysTitle = new Label(forestRun.getNumRunways + " runway configurations")
    val exitsTitle = new Label(forestRun.getNumExits + " exits")
    val airportIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/airportinv.png"), h * 0.25, h * 0.25, true, true)
    }
    val runwaysIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/runwaysinv.png"), h * 0.25, h * 0.25, true, true)
    }
    val exitsIcon = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/exitsinv.png"), h * 0.25, h * 0.25, true, true)
    }
    val airportBox = new HBox(spacing = w * 0.01)
    airportBox.children.addAll(airportIcon, airportTitle)
    val runwaysBox = new HBox(spacing = w * 0.01)
    runwaysBox.children.addAll(runwaysIcon, runwaysTitle)
    val exitsBox = new HBox(spacing = w * 0.01)
    exitsBox.children.addAll(exitsIcon, exitsTitle)

    val cellVBox = new VBox(spacing = h * 0.01)
    cellVBox.children.addAll(airportBox, runwaysBox, exitsBox)

    val cellPane = new AnchorPane
    cellPane.prefWidth = w
    cellPane.prefHeight = h
    cellPane.children.add(cellVBox)
    AnchorPane.setAnchors(cellVBox, cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d)

    cellPane
  }

  /** Creates Run Information display module */
  private def genRunInfoCell(w: Double, h: Double) = {
    val accuracyLabel = new Label("Prediction Accuracy")
    val accuracyIndicator = new ProgressIndicator {
      prefWidth = h * 0.9
      prefHeight = h * 0.9
      val accuracy = forestRun.getAccuracy
      progress = accuracy / 100D
    }
    accuracyIndicator.style = " -fx-progress-color: gold;"
    val accuracyBox = new VBox(spacing = h * 0.01)
    accuracyBox.children.addAll(accuracyLabel, accuracyIndicator)
    val trainLabel = new Label(forestRun.getTrainCount + " training records")
    val testLabel = new Label(forestRun.getTestCount + " training records")
    val durationLabel = new Label("Run took " + controller.getMinSecStr(forestRun.getDuration))
    val dateLabel = new Label(forestRun.getDate)

    val labelBox = new VBox(spacing = h * 0.01)
    labelBox.children.addAll(trainLabel, testLabel, durationLabel, dateLabel)

    val cellHBox = new HBox(spacing = w * 0.01)
    cellHBox.children.addAll(accuracyBox, labelBox)

    val cellPane = new AnchorPane
    cellPane.prefWidth = w
    cellPane.prefHeight = h
    cellPane.children.add(cellHBox)
    AnchorPane.setAnchors(cellHBox, cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d)

    cellPane
  }

  /** Creates Feature Importances display module */
  private def genFeaturePieCell(w: Double, h: Double) = {

    val chartList = forestRun.forestImportances.zipWithIndex.map {
      case (importance, index) =>
        new javafx.scene.chart.PieChart.Data(controller.formatFeature(forestRun.getFeatureName(index)), importance)
    }.toList

    val chartData = FXCollections.observableList(ListBuffer(chartList: _*))

    val featurePie = new javafx.scene.chart.PieChart {
      override def layoutChartChildren(top: Double, left: Double, contentWidth: Double, contentHeight: Double) {
        if (getLabelsVisible()) {
          getData().foreach(d => {
            val opTextArr = d.chart.value.lookupAll(".chart-pie-label").toList.
              filter(n => n.isInstanceOf[Text] && (n.asInstanceOf[Text]).text.value.contains(d.name.value))
            if (!opTextArr.isEmpty) {
              (opTextArr.head.asInstanceOf[Text]).text = d.getName() + " " +
                BigDecimal(d.getPieValue() * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP) + " %"
            }
          })
        }
        super.layoutChartChildren(top, left, contentWidth, contentHeight)
      }
    }
    featurePie.setTitle("Forest Feature Importances")
    featurePie.setData(chartData)
    featurePie.setStartAngle(180)
    featurePie.setClockwise(false)
    featurePie.setPrefHeight(h * 0.95)
    featurePie.setPrefWidth(w * 0.95)
    featurePie.setAnimated(true)

    val cellPane = new AnchorPane
    cellPane.prefWidth = w
    cellPane.prefHeight = h
    cellPane.children.add(featurePie)
    AnchorPane.setAnchors(featurePie, cellPane.getPrefWidth * 0.05d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d,
      cellPane.getPrefWidth * 0.01d)

    cellPane
  }

  /** Fills tier with relevant cells. Width and height provided due to HBox not having set sizes before being added to stage */
  private def fillTierTwo(tier: HBox, w: Double, h: Double) = {
    val forestCell = genForestCell(w * 0.9, h)
    forestCell.styleClass.add("brightTwo")
    tier.children.addAll(forestCell)

    tier
  }

  /** Creates Tree Viewer display module */
  private def genForestCell(w: Double, h: Double) = {

    val forestPane = new AnchorPane
    val treeViewPane = controller.genTreePane(0) // default first tree
    treeViewPane.prefHeight = h * 0.98
    treeViewPane.prefWidth = w * 0.9
    treeViewPane.hvalue = 0.5

    val treeBranchLabel = new Label("Left branch = YES,  Right branch = NO")
    treeBranchLabel.styleClass.add("importantTip")

    val viewerBox = new VBox
    viewerBox.children = List(treeBranchLabel, treeViewPane)
    viewerBox.prefWidth = w

    val hudBox = new HBox

    val numTrees = forestRun.getForestNumTrees
    val treeSelector = new ComboBox[Int] {
      promptText = "Select Tree..."
      editable = false
      items = ObservableBuffer(1 to numTrees)
      selectionModel().selectFirst()
      onAction = (event: ActionEvent) => {
        val paneAndLabel = controller.onSelectTree(selectionModel().getSelectedItem - 1)
        val newPane = paneAndLabel._1
        newPane.prefHeight = h * 0.98
        newPane.prefWidth = w * 0.9
        newPane.hvalue = 0.5
        viewerBox.children.remove(viewerBox.children.last)
        viewerBox.children.add(newPane)
      }
    }

    hudBox.children.addAll(treeSelector, treeBranchLabel)

    treeSelector.styleClass.add("selector")

    forestPane.children.addAll(viewerBox, hudBox)
    AnchorPane.setLeftAnchor(hudBox, 0)

    val cellPane = new AnchorPane
    cellPane.prefWidth = w * 0.9
    cellPane.prefHeight = h
    cellPane.children.add(forestPane)
    AnchorPane.setAnchors(forestPane, cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d)

    cellPane
  }

  /** Fills tier with relevant cells. Width and height provided due to HBox not having set sizes before being added to stage */
  private def fillTierThree(tier: HBox, w: Double, h: Double) = {
    val mapCell = genMapCell(w)
    mapCell.styleClass.add("brightTwo")
    tier.children.add(mapCell)
    tier
  }

  /** Creates Path Viewer display module */
  private def genMapCell(w: Double): AnchorPane = {
    val scPane = new ScrollPane
    scPane.prefWidth = w * 0.8
    scPane.prefHeight = w * 0.5
    val airportMap = controller.genMap(forestRun.getAirportCode, w)
    val diagramBox = new VBox
    diagramBox.children.add(new Label("Click on the left or right sides of the diagram to see other samples"))
    diagramBox.children.add(airportMap)
    scPane.content = diagramBox

    val cellPane = new AnchorPane
    cellPane.prefWidth = w * 0.9
    cellPane.prefHeight = w * 0.6
    cellPane.children.add(scPane)
    AnchorPane.setAnchors(scPane, cellPane.getPrefWidth * 0.05d,
      cellPane.getPrefWidth * 0.05d,
      cellPane.getPrefWidth * 0.05d,
      cellPane.getPrefWidth * 0.05d)

    cellPane
  }

  /** Fills tier with relevant cells. Width and height provided due to HBox not having set sizes before being added to stage */
  private def fillTierFour(tier: HBox, w: Double, h: Double) = {
    val timesCell = genTimesCell(w * 0.32D, h)
    val compCell = genCompCell(w * 0.55D, h)
    timesCell.styleClass.add("brightFour")
    compCell.styleClass.add("brightOne")
    tier.children.addAll(timesCell, compCell)
    tier
  }

  /** Creates ROT and TT display module */
  private def genTimesCell(w: Double, h: Double) = {

    val rotTimeCategories = ObservableBuffer("ROT")
    val rotxAxis = NumberAxis("Seconds", 0.0d, 5d, 1.0d)
    val rotyAxis = CategoryAxis(rotTimeCategories)
    def rotxyData(ys: Seq[Number]) = ObservableBuffer(rotTimeCategories zip ys map (xy => XYChart.Data(xy._2, xy._1)))
    val rotseries1 = XYChart.Series("Predicted", rotxyData(Seq(forestRun.optExtracted.actualROT)))
    val rotseries2 = XYChart.Series("Optimal", rotxyData(Seq(forestRun.optExtracted.optROT)))

    val rotBarChart = new BarChart(rotxAxis, rotyAxis) {
      title = "ROT reduction"
      data() ++= Seq(rotseries1, rotseries2)
      categoryGap = 25.0d
      prefWidth = w * 0.49
      prefHeight = h
    }

    val ttTimeCategories = ObservableBuffer("TT")
    val ttxAxis = NumberAxis("Seconds", 0.0d, 200.0d, 50.0d)
    val ttyAxis = CategoryAxis(ttTimeCategories)
    def ttxyData(ys: Seq[Number]) = ObservableBuffer(ttTimeCategories zip ys map (xy => XYChart.Data(xy._2, xy._1)))
    val ttseries1 = XYChart.Series("Predicted", ttxyData(Seq(forestRun.optExtracted.actualTT)))
    val ttseries2 = XYChart.Series("Optimal", ttxyData(Seq(forestRun.optExtracted.optTT)))

    val ttBarChart = new BarChart(ttxAxis, ttyAxis) {
      title = "TT reduction"
      data() ++= Seq(ttseries1, ttseries2)
      categoryGap = 25.0d
      prefWidth = w * 0.49
      prefHeight = h
    }

    val chartBox = new HBox()
    chartBox.children.addAll(rotBarChart, ttBarChart)

    val cellPane = new AnchorPane
    cellPane.prefWidth = w
    cellPane.prefHeight = h
    cellPane.children.add(chartBox)
    AnchorPane.setAnchors(chartBox, cellPane.getPrefWidth * 0.001d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d)

    cellPane
  }

  /** Creates Aggregate costs display module */
  private def genCompCell(w: Double, h: Double) = {

    val socCosts = Seq("Societal")
    val socXAxis = CategoryAxis(socCosts)
    val socYAxis = NumberAxis("$", 0.0d, 100.0d, 10.0d)
    def socxyData(ys: Seq[Number]) = ObservableBuffer(socCosts zip ys map (xy => XYChart.Data(xy._1, xy._2)))
    val socSeries1 = new XYChart.Series[String, Number] {
      name = "Passenger Time"
      data = socxyData(Seq(forestRun.optExtracted.actualPcost - forestRun.optExtracted.optPcost))
    }
    val socSeries2 = new XYChart.Series[String, Number] {
      name = "Environmental"
      data = socxyData(Seq(forestRun.optExtracted.actualEnvCost - forestRun.optExtracted.optEnvCost))
    }
    val socChart = new StackedBarChart[String, Number](socXAxis, socYAxis) {
      title = "Societal Impact"
      data() ++= Seq(socSeries1, socSeries2)
      categoryGap = w * 0.2
      prefWidth = w * 0.5
      prefHeight = h
      legendVisible = true
      legendSide = Side.Right
    }

    val aaCosts = Seq("Airport/Airline")
    val aaXAxis = CategoryAxis(aaCosts)
    val aaYAxis = NumberAxis("$", 0.0d, 35.0d, 5.0d)
    def aaxyData(ys: Seq[Number]) = ObservableBuffer(aaCosts zip ys map (xy => XYChart.Data(xy._1, xy._2)))
    val aaSeries1 = new XYChart.Series[String, Number] {
      name = "Fuel"
      data = aaxyData(Seq(forestRun.optExtracted.actualFuelCost - forestRun.optExtracted.optFuelCost))
    }
    val aaSeries2 = new XYChart.Series[String, Number] {
      name = "Crew"
      data = aaxyData(Seq(forestRun.optExtracted.actualCrewCost - forestRun.optExtracted.optCrewCost))
    }
    val aaSeries3 = new XYChart.Series[String, Number] {
      name = "Maintenance"
      data = aaxyData(Seq(forestRun.optExtracted.actualMaintCost - forestRun.optExtracted.optMaintCost))
    }
    val aaSeries4 = new XYChart.Series[String, Number] {
      name = "Ownership"
      data = aaxyData(Seq(forestRun.optExtracted.actualAOCost - forestRun.optExtracted.optAOCost))
    }
    val aaSeries5 = new XYChart.Series[String, Number] {
      name = "Other"
      data = aaxyData(Seq(forestRun.optExtracted.actualOtherCost - forestRun.optExtracted.optOtherCost))
    }
    val aaSeries6 = new XYChart.Series[String, Number] {
      name = "ROT benefits"
      data = aaxyData(Seq(forestRun.optExtracted.savings))
    }
    val aaChart = new StackedBarChart[String, Number](aaXAxis, aaYAxis) {
      title = "Airport/Airline Impact"
      data() ++= Seq(aaSeries1, aaSeries2, aaSeries3, aaSeries4, aaSeries5, aaSeries6)
      categoryGap = w * 0.2
      prefWidth = w * 0.5
      prefHeight = h
      legendVisible = true
      legendSide = Side.Right
    }

    val chartBox = new HBox()
    chartBox.children.addAll(socChart, aaChart)
    chartBox.prefWidth = w
    chartBox.prefHeight = h

    val cellPane = new AnchorPane
    cellPane.prefWidth = w
    cellPane.prefHeight = h
    cellPane.children.add(chartBox)
    AnchorPane.setAnchors(chartBox, cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d,
      cellPane.getPrefWidth * 0.005d)

    cellPane

  }

}