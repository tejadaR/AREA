/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

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
import javafx.scene.control.ScrollPane.ScrollBarPolicy

/** View implementation */
class OptionsView(controller: => OptionsController, stageWidth: Double, stageHeight: Double) {
  val tab = new Tab
  val viewBox = new HBox
  tab.content = viewBox
  tab.text = "AREA"
  viewBox.styleClass.add("container")

  val analysisBox = genAnalysisBox(stageWidth * 0.6, stageHeight)
  val resultsBox = genResultBox(stageWidth * 0.4, stageHeight)
  analysisBox.styleClass.add("module")
  resultsBox.styleClass.add("module")
  viewBox.children.addAll(analysisBox, resultsBox)

  /** Generates main analysis module*/
  private def genAnalysisBox(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("Airport Runway Exit Analysis")
    this.prefWidth = w
    this.prefHeight = h

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
    airportBox.spacing = (w * 0.05)

    val treeBox = new HBox
    val treeLabel = new Label("Number of Trees: ")
    val treeSelector = new ChoiceBox[Int]
    treeSelector.items = ObservableBuffer(controller.model.treeSeq)
    treeSelector.getSelectionModel.selectFirst
    treeSelector.styleClass.add("selector")
    treeBox.children.addAll(treeLabel, treeSelector)
    treeBox.spacing = (w * 0.05)

    val depthBox = new HBox
    val depthLabel = new Label("Max Tree Depth: ")
    val depthSelector = new ChoiceBox[Int]
    depthSelector.items = ObservableBuffer(controller.model.depthSeq)
    depthSelector.getSelectionModel.selectFirst
    depthSelector.styleClass.add("selector")
    depthBox.children.addAll(depthLabel, depthSelector)
    depthBox.spacing = (w * 0.05)

    val cbPane = new TilePane {
      prefColumns = 2
      tileAlignment = Pos.TopLeft
      children = List(new CheckBox("Runway") {
        id = "runway"
        selected = true
        disable = true
      },
        new CheckBox("Origin Airport") {
          id = "depAirport"
          selected = true
        },
        new CheckBox("Aircraft Type") {
          id = "aircraftType"
          selected = true
        },
        new CheckBox("Arrival Terminal") {
          id = "arrGate"
          selected = true
        },
        new CheckBox("Touchdown Latitude") {
          id = "touchdownLat"
          selected = true
        },
        new CheckBox("Touchdown Longitude") {
          id = "touchdownLong"
          selected = true
        },
        new CheckBox("Hour") {
          id = "hour"
          selected = true
        },
        new CheckBox("Day of Week") {
          id = "day"
          selected = true
        },
        new CheckBox("Deceleration(m/s\u00B2)") {
          id = "decel"
          selected = true
        },
        new CheckBox("Airline") {
          id = "carrier"
          selected = true
        },
        new CheckBox("Traffic") {
          id = "traffic"
          selected = true
        })
      hgap = w * 0.01
      vgap = h * 0.01
    }
    val cbAnchor = new AnchorPane
    cbAnchor.children.add(cbPane)
    AnchorPane.setAnchors(cbPane, bodyPane.getPrefHeight * 0.01, bodyPane.getPrefHeight * 0.01,
      bodyPane.getPrefHeight * 0.01, bodyPane.getPrefHeight * 0.01)
    cbAnchor.styleClass.add("subModule")

    val runButton = new Button("Run") {
      onAction = (ae: ActionEvent) => {
        controller.onRun(cbPane, airportSelector.value.apply,
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
    statusLabel.styleClass.add("statusLabel")
    statusPane.children.add(statusLabel)

    val areaPane = new AnchorPane
    areaPane.prefWidth = w
    areaPane.prefHeight = h * 0.3
    val paramBox = new VBox
    paramBox.spacing = h * 0.025
    paramBox.children.addAll(airportBox, treeBox, depthBox)

    val controlBox = new HBox
    controlBox.spacing = w * 0.025
    controlBox.children.addAll(paramBox, cbAnchor)
    val areaBox = new VBox
    areaBox.spacing = h * 0.025
    areaBox.children.addAll(controlBox, runButton, statusPane, runPb)
    areaPane.children.add(areaBox)

    AnchorPane.setLeftAnchor(areaBox, areaPane.getPrefWidth * 0.025d)

    val singleTestModule = genSingleTestModule(w, h * 0.35)

    bodyBox.children.addAll(areaPane, singleTestModule)
  }

  /** Generates single-test module*/
  private def genSingleTestModule(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("Model Test Module")
    this.prefWidth = w
    this.prefHeight = h

    headerPane.children.add(titleLabel)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.1
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.9

    AnchorPane.setAnchors(titleLabel, 0, headerPane.getPrefWidth * 0.05d, 0, headerPane.getPrefWidth * 0.05d)
    AnchorPane.setTopAnchor(bodyBox, bodyPane.getPrefHeight * 0.05d)
    AnchorPane.setLeftAnchor(bodyBox, bodyPane.getPrefWidth * 0.025d)

    val loadPane = new AnchorPane
    val loadBox = new HBox
    val selectBox = new HBox
    val selectLabel = new Label("Select Model: ")
    val statusLabel = new Label("Module Status")
    statusLabel.styleClass.add("statusLabel")
    val modelSelector = new ChoiceBox[String]
    modelSelector.items = ObservableBuffer(controller.model.getModels)
    modelSelector.styleClass.add("selector")
    modelSelector.prefWidth = w * 0.2
    selectBox.children.addAll(selectLabel, modelSelector)
    val loadButton = new Button("Load") {
      onAction = (ae: ActionEvent) => {
        if (modelSelector.value.isNotNull.apply)
          controller.onLoad(modelSelector.value.apply)
        else
          statusLabel.text = "No model selected, unable to load."
      }
    }
    loadBox.children.addAll(selectBox, loadButton, statusLabel)
    loadBox.spacing = bodyPane.getPrefWidth * 0.1
    loadPane.children.add(loadBox)

    val paramPane = new TilePane { tileAlignment = Pos.TopLeft }
    paramPane.hgap = bodyPane.getPrefWidth * 0.05
    paramPane.vgap = bodyPane.getPrefHeight * 0.05
    paramPane.styleClass.add("subModule")
    paramPane.visible = false

    val testPane = new AnchorPane
    val testBox = new HBox
    testBox.spacing = w * 0.01
    val testButton = new Button("Predict") {
      disable = true
      onAction = (ae: ActionEvent) => {
        controller.onTest(paramPane)
      }
    }

    val mapButton = new Button("Diagram") {
      disable = true
      onAction = (ae: ActionEvent) => {
        val airportCode = controller.model.modelMap.head._2._2.getAirportCode
        controller.onOpenDiagram(airportCode)
      }
    }

    val testLabel = new Label("")
    testBox.children.addAll(testButton, testLabel)
    val lowerControls = new HBox
    lowerControls.spacing = w * 0.03
    lowerControls.children.addAll(testBox, mapButton)
    testPane.children.add(lowerControls)

    bodyBox.children.addAll(loadPane, paramPane, testPane)
    bodyBox.prefWidth = w
    bodyBox.prefHeight = h * 0.9
    bodyBox.spacing = h * 0.06
  }

  /** Generates results-chooser module*/
  private def genResultBox(w: Double, h: Double) = new VBox with TitledModuleV {
    val titleLabel = new Label("Results")
    this.prefWidth = w
    this.prefHeight = h

    headerPane.children.add(titleLabel)
    headerPane.prefWidth = w
    headerPane.minHeight = h * 0.05
    headerPane.styleClass.add("title")
    bodyPane.prefWidth = w
    bodyPane.prefHeight = h * 0.95

    AnchorPane.setAnchors(titleLabel, 0, headerPane.getPrefWidth * 0.3d, 0, headerPane.getPrefWidth * 0.3d)
    AnchorPane.setLeftAnchor(bodyBox, bodyPane.getPrefWidth * 0.05d)
    AnchorPane.setTopAnchor(bodyBox, bodyPane.getPrefHeight * 0.05d)

    val clearButton = new Button("Clear All") {
      onAction = (ae: ActionEvent) => {
        controller.onClearAll
      }
    }

    val resultsPane = new TilePane
    resultsPane.prefColumns = 3
    resultsPane.hgap = w * 0.01
    resultsPane.vgap = h * 0.01
    resultsPane.setPrefWidth(bodyPane.getPrefWidth * 0.8)
    resultsPane.children = controller.genButtonSeq
    val scPane = new ScrollPane
    scPane.content = resultsPane
    scPane.setHbarPolicy(ScrollBarPolicy.NEVER)
    scPane.setVbarPolicy(ScrollBarPolicy.ALWAYS)
    scPane.setMaxWidth(bodyPane.getPrefWidth * 0.8)
    scPane.setPrefHeight(bodyPane.getPrefHeight * 0.7)
    scPane.setMaxHeight(bodyPane.getPrefHeight * 0.8)

    val resultsBox = new VBox {
      children = List(clearButton, scPane)
    }

    bodyBox.children.add(resultsBox)
    bodyBox.prefWidth = w * 0.9
    bodyBox.prefHeight = h * 0.95
  }

}
