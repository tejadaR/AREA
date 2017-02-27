package rtejada.projects.AREA.view

import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.scene.Parent
import scalafx.scene.Node
import rtejada.projects.AREA.controller.OptionsController
import scalafx.collections.ObservableBuffer
import scalafx.geometry.{ Pos, Insets }
import scalafx.event.ActionEvent
import scalafx.scene.image.{ Image, ImageView }

/** View implementation */
class OptionsView(controller: => OptionsController, stageWidth: Double, stageHeight: Double) {
  val tab = new Tab
  val viewBox = new HBox
  tab.content = viewBox
  tab.text = "AREA"
  viewBox.styleClass.add("container")

  val analysisBox = genAnalysisBox(stageWidth * 0.65, stageHeight)
  val resultsBox = genResultBox(stageWidth * 0.35, stageHeight)
  analysisBox.styleClass.add("module")
  resultsBox.styleClass.add("module")
  viewBox.children.addAll(analysisBox, resultsBox)

  private def genAnalysisBox(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("Airport Runway Exit Analysis")

    headerPane.children.add(titleLabel)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.05
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.95

    AnchorPane.setAnchors(titleLabel, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)
    AnchorPane.setTopAnchor(bodyBox, bodyPane.getPrefHeight * 0.05d)

    val airportBox = new HBox
    val airportLabel = new Label("Airport: ")
    val airportSelector = new ChoiceBox[String]
    airportSelector.items = ObservableBuffer(controller.model.airportSeq)
    airportSelector.getSelectionModel.selectFirst
    airportSelector.styleClass.add("selector")
    airportBox.children.addAll(airportLabel, airportSelector)

    val treeBox = new HBox
    val treeLabel = new Label("Number of Trees: ")
    val treeSelector = new ChoiceBox[Int]
    treeSelector.items = ObservableBuffer(controller.model.treeSeq)
    treeSelector.getSelectionModel.selectFirst
    treeSelector.styleClass.add("selector")
    treeBox.children.addAll(treeLabel, treeSelector)

    val depthBox = new HBox
    val depthLabel = new Label("Max Tree Depth: ")
    val depthSelector = new ChoiceBox[Int]
    depthSelector.items = ObservableBuffer(controller.model.depthSeq)
    depthSelector.getSelectionModel.selectFirst
    depthSelector.styleClass.add("selector")
    depthBox.children.addAll(depthLabel, depthSelector)

    val areaPane = new AnchorPane
    areaPane.prefWidth = w
    areaPane.prefHeight = h * 0.3
    val areaBox = new VBox
    areaBox.spacing = h * 0.025
    AnchorPane.setLeftAnchor(areaBox, areaPane.getPrefWidth * 0.05d)

    val runButton = new Button("Run") {
      onAction = (ae: ActionEvent) => {
        controller.onRun(airportSelector.value.apply,
          treeSelector.value.apply, depthSelector.value.apply)
      }
    }
    val runPb = new ProgressBar {
      maxWidth = 200
      progress = 0
      visible = false
    }
    val statusPane = new AnchorPane
    val statusLabel = new Label("Standing by")
    statusPane.children.add(statusLabel)

    areaBox.children.addAll(airportBox, treeBox, depthBox, runButton, statusPane, runPb)
    areaPane.children.add(areaBox)

    val singleTestModule = genSingleTestModule(w, h * 0.35)

    bodyBox.children.addAll(areaPane, singleTestModule)
  }

  private def genSingleTestModule(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("Model Test Module")

    headerPane.children.add(titleLabel)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.1
    headerPane.styleClass.add("title")

    AnchorPane.setAnchors(titleLabel, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)

    val loadPane = new AnchorPane
    val loadBox = new HBox
    val selectBox = new HBox
    val selectLabel = new Label("Select Model: ")
    val modelSelector = new ChoiceBox[String]
    modelSelector.items = ObservableBuffer(controller.model.getModels)
    modelSelector.styleClass.add("selector")
    selectBox.children.addAll(selectLabel, modelSelector)
    val refreshButton = new Button {
      graphic = new ImageView {
        image = new Image(this.getClass.getResourceAsStream("/img/sync.png"), h * 0.08, h * 0.08, true, true)
      }
      onAction = (ae: ActionEvent) => {
        modelSelector.items = ObservableBuffer(controller.model.getModels)
      }
    }
    val loadButton = new Button("Load") {
      onAction = (ae: ActionEvent) => {
        controller.onLoad(modelSelector.value.apply)
      }
    }
    val statusLabel = new Label("LoadStatusLabel")
    loadBox.children.addAll(selectBox, refreshButton, loadButton, statusLabel)
    loadPane.children.add(loadBox)

    val paramPane = new TilePane

    val testPane = new AnchorPane
    val testBox = new HBox
    val testButton = new Button("Predict") {
      disable = true
      onAction = (ae: ActionEvent) => {
        controller.onTest(paramPane)
      }
    }
    val testLabel = new Label("")
    testBox.children.addAll(testButton, testLabel)
    testPane.children.add(testBox)

    bodyBox.children.addAll(loadPane, paramPane, testPane)
    bodyBox.prefWidth = w
    bodyBox.prefHeight = h * 0.9

    this.prefWidth = w
    this.prefHeight = h
  }

  private def genResultBox(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("RESULTS")

    headerPane.children.add(titleLabel)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.05
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.95

    AnchorPane.setAnchors(titleLabel, 0, headerPane.getPrefWidth * 0.3d, 0, headerPane.getPrefWidth * 0.3d)
    AnchorPane.setLeftAnchor(bodyBox, bodyPane.getPrefWidth * 0.05d)
    AnchorPane.setTopAnchor(bodyBox, bodyPane.getPrefHeight * 0.05d)

    val resultsPane = new TilePane
    resultsPane.children = controller.onRefreshResults
    val controlBox = new HBox
    val refreshButton = new Button {
      graphic = new ImageView {
        image = new Image(this.getClass.getResourceAsStream("/img/sync.png"), h * 0.03, h * 0.03, true, true)
      }
      onAction = (ae: ActionEvent) => {
        resultsPane.children = controller.onRefreshResults
      }
    }
    controlBox.children.addAll(refreshButton)

    bodyBox.children.addAll(controlBox, resultsPane)
    bodyBox.prefWidth = w
    bodyBox.prefHeight = h * 0.95
  }

}
