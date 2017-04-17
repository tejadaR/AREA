/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.controller

import scalafx.Includes._
import rtejada.projects.AREA.model.AREAModel
import rtejada.projects.AREA.view.OptionsView
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import scala.util.Random
import scalafx.application.Platform
import scalafx.collections.ObservableBuffer
import scalafx.scene.layout.{ HBox, VBox, TilePane }
import scalafx.scene.control._
import java.util.regex.Pattern
import scalafx.scene.control.TextFormatter
import scalafx.util.converter.DoubleStringConverter
import scalafx.scene.control.TextFormatter.Change
import scalafx.scene.Node
import scalafx.scene.Parent
import java.io.File
import rtejada.projects.AREA.model.ForestRun
import scalafx.event.ActionEvent
import rtejada.projects.AREA.view.ResultsView
import scalafx.scene.layout.StackPane
import scalafx.scene.image.ImageView
import scalafx.scene.image.Image
import scalafx.geometry.Pos
import scalafx.scene.layout.AnchorPane
import org.apache.commons.io.FileUtils
import scalafx.stage.Stage
import scalafx.scene.Scene
import scalafx.scene.input.MouseEvent
import javafx.animation.{ Timeline, KeyFrame }
import javafx.util.Duration
import scalafx.scene.canvas.Canvas
import scalafx.scene.paint.Color

/** Controller implementation */
class OptionsController(mlModel: => AREAModel, view: => OptionsView, stageW: Double, stageH: Double) {
  val model = mlModel

  /**
   * Creates a new stage with the corresponding airport diagram, providing
   *  functionality to fill out coordinate fields  by clicking on the map
   */
  def onOpenDiagram(airportCode: String, prediction: Option[String]): Unit = {

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

    prediction match {
      case Some(predictedExit) => {
        val loadedLinks = model.loadLinks(airportCode)
        println("ALLEXITS")
        loadedLinks._1.show(150)
        val predictionInfo = loadedLinks._1.filter("LinkID == '$predictedExit'")
        println("predictedExit: " + predictedExit)
        predictionInfo.show

        val srcLat = predictionInfo.head().getAs[String]("srcLatitude").toDouble
        val srcLong = predictionInfo.head().getAs[String]("srcLongitude").toDouble
        val dstLat = predictionInfo.head().getAs[String]("dstLatitude").toDouble
        val dstLong = predictionInfo.head().getAs[String]("dstLongitude").toDouble

        val drawSrcLat = Math.abs(srcLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawSrcLong = Math.abs(srcLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)
        val drawDstLat = Math.abs(dstLat - startPoint._2) *
          airportImg.getImage.getHeight / Math.abs(endPoint._2 - startPoint._2)
        val drawDstLong = Math.abs(dstLong - startPoint._1) *
          airportImg.getImage.getWidth / Math.abs(endPoint._1 - startPoint._1)

        gc.lineWidth = 1
        gc.setStroke(Color.Yellow)
        gc.strokeOval(drawSrcLong, drawSrcLat, 4, 4)
        gc.strokeOval(drawDstLong, drawDstLat, 4, 4)

        gc.setStroke(Color.Gold)
        gc.strokeText(predictedExit, (drawSrcLong + drawDstLong) / 2, (drawSrcLat + drawDstLat) / 2)

        gc.lineWidth = 3
        gc.setStroke(Color.Blue)
        gc.strokeLine(drawSrcLong, drawSrcLat, drawDstLong, drawDstLat)
      }
      case _ => println("Diagram without prediction")
    }

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
    val clickEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        val xRatio = event.x / airportImg.getImage.getWidth
        val yRatio = event.y / airportImg.getImage.getHeight
        val long = BigDecimal(startPoint._1 + ((endPoint._1 - startPoint._1) * xRatio)).setScale(5, BigDecimal.RoundingMode.HALF_UP)
        val lat = BigDecimal(startPoint._2 + ((endPoint._2 - startPoint._2) * yRatio)).setScale(5, BigDecimal.RoundingMode.HALF_UP)
        val longField = view.analysisBox.singleTestModule.paramPane.children.toArray.
          map(_.asInstanceOf[javafx.scene.layout.HBox]).
          filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.TextField]).
          map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.TextField]).
          find(node => node.getId == "touchdownLongselector")
        val latField = view.analysisBox.singleTestModule.paramPane.children.toArray.
          map(_.asInstanceOf[javafx.scene.layout.HBox]).
          filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.TextField]).
          map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.TextField]).
          find(node => node.getId == "touchdownLatselector")
        longField match {
          case Some(field) => field.text = long.toString
          case None        =>
        }
        latField match {
          case Some(field) => field.text = lat.toString
          case None        =>
        }
        imgStage.close()
        event
      }
    }
    airportImg.setOnMouseMoved(moveEvent)
    airportImg.setOnMouseClicked(clickEvent)
    imgStage.scene = imgScene
    imgStage.onCloseRequest = handle { imgStage.close() }
    imgStage.showAndWait
  }

  /** Clears a single run from all view controls, deleting all associated files*/
  def onClearRun(airportCode: String, id: String) {
    val runFuture = Future {
      def deletDir(file: File): Unit = {
        if (file.isDirectory)
          file.listFiles.foreach(deletDir)
        if (file.exists && !file.delete)
          throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
      FileUtils.deleteQuietly(new File("output/features" + id + ".json"))
      FileUtils.deleteQuietly(new File("output/randomForest" + id + ".txt"))
      FileUtils.deleteQuietly(new File("output/runInfo" + id + ".json"))
      deletDir(new File("trained/" + airportCode + "model" + id))
    }
    runFuture.onComplete {
      case Success(s) => {
        Platform.runLater {
          {
            onRefresh
            view.analysisBox.singleTestModule.modelSelector.items = ObservableBuffer(model.getModels)

          }
        }
      }
      case Failure(e) => { Platform.runLater(e.printStackTrace) }
    }
  }

  /** Clears every run from all view controls, deleting all associated files*/
  def onClearAll {
    val runFuture = Future {
      def deletDir(file: File): Unit = {
        if (file.isDirectory)
          file.listFiles.foreach(deletDir)
        if (file.exists && !file.delete)
          throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
      deletDir(new File("trained/"))
      deletDir(new File("output/"))
    }
    runFuture.onComplete {
      case Success(s) => {
        Platform.runLater {
          {
            onRefresh
            view.analysisBox.singleTestModule.modelSelector.items = ObservableBuffer(model.getModels)
          }
        }
      }
      case Failure(e) => { Platform.runLater(e.printStackTrace) }
    }
  }

  /** Refreshes runs in file to appear in results and in the single-test-module*/
  def onRefresh: Unit = {
    val buttons = genButtonSeq
    view.analysisBox.singleTestModule.modelSelector.items = ObservableBuffer(model.getModels)
    view.resultsBox.resultsPane.children = buttons
  }

  /** Generates viewing buttons for each model in file*/
  def genButtonSeq: Seq[AnchorPane] = {
    val dir = new File("output")
    val fileSeq = if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toSeq else Seq[File]()
    val filteredSeq = fileSeq.filter { file =>
      {
        val runID = file.getName.filter(_.isDigit)
        if (file.getName.contains("runInfo")) {
          new File("output/features" + runID + ".json").exists &&
            new File("output/randomForest" + runID + ".txt").exists
        } else if (file.getName.contains("features")) {
          new File("output/runInfo" + runID + ".json").exists &&
            new File("output/randomForest" + runID + ".txt").exists
        } else {
          new File("output/features" + runID + ".json").exists &&
            new File("output/runInfo" + runID + ".json").exists
        }
      }
    }
    for (file <- filteredSeq if file.getName.contains("runInfo")) yield {
      val runID = file.getName.filter(_.isDigit)
      val forestRun = new ForestRun("output/features" + runID + ".json",
        "output/randomForest" + runID + ".txt",
        "output/runInfo" + runID + ".json",
        "output/optimization" + runID + ".json")
      val resultsController: ResultsController = new ResultsController(model, forestRun, stageW, stageH)
      val resultsView: ResultsView = new ResultsView(resultsController, forestRun, stageW, stageH)
      resultsView.tab.id = runID
      val pane = new AnchorPane
      val closeButton = new Button {
        graphic = new ImageView {
          image = new Image(this.getClass.getResourceAsStream("/img/close.png"),
            stageH * 0.01, stageH * 0.01, true, true)
        }
        maxWidth = stageH * 0.005
        maxHeight = stageH * 0.005
        onAction = (ae: ActionEvent) => {
          onClearRun(forestRun.getAirportCode, runID)
        }
      }
      val mainButton = new Button {
        text = "Run" + runID + System.lineSeparator() + forestRun.getAirportCode +
          ", Acc: " + BigDecimal(forestRun.getAccuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP) + "%"
        onAction = (ae: ActionEvent) => {
          val optTab = view.tab.getTabPane.getTabs.find(_.id.value == runID)
          if (!optTab.isDefined) {
            view.tab.getTabPane.getTabs.add(resultsView.tab)
            view.tab.getTabPane.getSelectionModel.select(resultsView.tab)
          } else view.tab.getTabPane.getSelectionModel.select(optTab.get)
        }
        prefWidth = stageW * 0.1
        prefHeight = stageH * 0.06
      }
      pane.children.addAll(mainButton, closeButton)
      AnchorPane.setRightAnchor(closeButton, 0)
      pane
    }
  }

  /** Gathers the selected parameters and defers testing to MLModel*/
  def onTest(paramGrid: TilePane) {
    model.optLoadedModel match {
      case Some(loadedModel) => {
        view.analysisBox.singleTestModule.testLabel.text = "Predicting..."
        view.analysisBox.singleTestModule.testButton.disable = true
        view.analysisBox.singleTestModule.mapButton.disable = true

        val testParamArray = loadedModel._3.featureExtracted.dropRight(1).map(feat => {
          if (feat.featureType == "categorical") {
            (feat.featureName, feat.featureType,
              view.analysisBox.singleTestModule.paramPane.children.toArray.
              map(_.asInstanceOf[javafx.scene.layout.HBox]).
              filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.ChoiceBox[String]]).
              map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.ChoiceBox[String]]).
              find(node => node.getId == feat.featureName + "selector").get.getValue)
          } else
            (feat.featureName, feat.featureType,
              view.analysisBox.singleTestModule.paramPane.children.toArray.
              map(_.asInstanceOf[javafx.scene.layout.HBox]).
              filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.TextField]).
              map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.TextField]).
              find(node => node.getId == feat.featureName + "selector").get.getText)
        })
        val runFuture = Future { model.testSingleRecord(testParamArray) }
        runFuture.onComplete {
          case Success(prediction) => {
            Platform.runLater {
              view.analysisBox.singleTestModule.testButton.disable = false
              view.analysisBox.singleTestModule.mapButton.disable = false
              view.analysisBox.singleTestModule.testLabel.text = "Prediction: Exit " + prediction
              onOpenDiagram(loadedModel._1.filter(!_.isDigit).replace("model", ""), Some(prediction))
            }
          }
          case Failure(e) => {
            Platform.runLater {
              view.analysisBox.singleTestModule.testButton.disable = false
              view.analysisBox.singleTestModule.mapButton.disable = false
              view.analysisBox.singleTestModule.testLabel.text = "testing failed"
              e.printStackTrace
            }
          }
        }

      }
      case _ => println("No model loaded")
    }
  }

  /** Loads the selected model and provides controllers for testing single records */
  def onLoad(modelName: String): Unit = {
    view.analysisBox.singleTestModule.statusLabel.text = "Loading model..."
    view.analysisBox.singleTestModule.paramPane.visible = false
    view.analysisBox.singleTestModule.paramPane.children.clear
    view.analysisBox.singleTestModule.testButton.disable = true
    view.analysisBox.singleTestModule.loadButton.disable = true
    view.analysisBox.singleTestModule.mapButton.disable = true
    view.analysisBox.singleTestModule.testLabel.text = ""
    val runFuture = Future { model.loadModel(modelName, view) }
    runFuture.onComplete {
      case Success(forestRun) => {
        Platform.runLater {
          val accuracy = forestRun.getAccuracy
          val airportCode = forestRun.getAirportCode
          val featureArray = forestRun.featureExtracted.dropRight(1)
          view.analysisBox.singleTestModule.statusLabel.text = " loaded: " + modelName + " " +
            airportCode + ", Acc: " + BigDecimal(accuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP) + "%"
          view.analysisBox.singleTestModule.testButton.disable = false
          view.analysisBox.singleTestModule.loadButton.disable = false
          view.analysisBox.singleTestModule.mapButton.disable = false
          view.analysisBox.singleTestModule.paramPane.visible = true
          val selectorArray = featureArray.map(feat => {
            val hBox = new HBox
            hBox.spacing = (view.analysisBox.singleTestModule.prefWidth.value * 0.015)
            val label = new Label(formatFeature(feat.featureName) + ":")
            if (feat.featureType == "categorical") {
              val selector = new ChoiceBox[String] { id = feat.featureName + "selector" }
              selector.items = feat.categories match {
                case None             => ObservableBuffer.empty
                case Some(categories) => ObservableBuffer(categories.toSeq.sorted)
              }
              selector.getSelectionModel.selectFirst
              hBox.children.addAll(label, selector)
              hBox
            } else {
              val validDouble = Pattern.compile("-?((\\d*)|(\\d+\\.\\d*))")
              val converter = new DoubleStringConverter
              val selector = new TextField { id = feat.featureName + "selector" }
              selector.prefWidth = view.analysisBox.singleTestModule.prefWidth.value * 0.1
              val filter: (Change) => Change = { change: Change =>
                {
                  val newText = change.getControlNewText
                  if (validDouble.matcher(newText).matches) change else null
                }
              }
              selector.textFormatter = new TextFormatter[Double](converter, 0.0, filter)
              hBox.children.addAll(label, selector)
              hBox
            }
          })
          selectorArray.foreach(selector => view.analysisBox.singleTestModule.paramPane.children.add(selector))
        }
      }
      case Failure(e) => {
        Platform.runLater {
          view.analysisBox.singleTestModule.loadButton.disable = false
          view.analysisBox.singleTestModule.statusLabel.text = "loading failed"
          e.printStackTrace
        }
      }
    }
  }

  /** Starts a new thread and runs a new model with the selected paramteres */
  def onRun(cbTilePane: TilePane, airport: String, treeNum: Int, depthNum: Int): Unit = {
    val cbList = cbTilePane.children.toList.
      map(node => node.asInstanceOf[javafx.scene.control.CheckBox]).
      filter(cb => cb.selected.apply)
    if (cbList.length < 5)
      view.analysisBox.statusLabel.text = "Please select 5 or more features to run."
    else {
      val featureList = cbList.map(cb => cb.getId)
      view.analysisBox.statusLabel.text = "Loading data..."
      view.analysisBox.runButton.disable = true
      view.analysisBox.runPb.visible = true
      val runFuture = Future { model.runModel(airport, treeNum, depthNum, featureList, view) }
      runFuture.onComplete {
        case Success(id) => {
          Platform.runLater {
            view.analysisBox.statusLabel.text = "Run Completed with id: " + id + "... standing by"
            view.analysisBox.runButton.disable = false
            view.analysisBox.runPb.progress = 0
            view.analysisBox.runPb.visible = false
            onRefresh
          }
        }
        case Failure(e) => {
          Platform.runLater {
            view.analysisBox.statusLabel.text = "Run Failed...Standing by" //e.getMessage
            view.analysisBox.runButton.disable = false
            view.analysisBox.runPb.progress = 0
            view.analysisBox.runPb.visible = false
          }
          e.printStackTrace
        }
      }
    }
  }

  /** Formats features for ease of use*/
  private def formatFeature(in: String): String = in match {
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

}