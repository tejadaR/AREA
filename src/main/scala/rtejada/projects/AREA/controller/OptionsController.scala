package rtejada.projects.AREA.controller

import scalafx.Includes._
import rtejada.projects.AREA.model.MLModel
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
import javafx.animation.Timeline
import javafx.animation.KeyFrame
import javafx.util.Duration

/** Controller implementation */
class OptionsController(mlModel: => MLModel, view: => OptionsView, stageW: Double, stageH: Double) {
  val model = mlModel

  def onMapSelected(airportCode: String): Unit = {
    val lcCode = airportCode match {
      case "KPHX" => "phx"
      case "KATL" => "atl"
      case "KDEN" => "den"
      case "KBWI" => "bwi"
    }

    val airportConfig = airportCode match {
      case "KPHX" => ("phx", (-112.030832, 33.442311), (-111.990, 33.426286))
      case "KATL" => ("atl", (-84.4491665, 33.651087), (-84.4041575, 33.61992152))
      case "KDEN" => ("den", (-104.7283332, 39.897435), (-104.616671, 39.826925))
      case "KBWI" => ("bwi", (-76.689681952, 39.1880947144), (-76.65158824, 39.1636909281))
    }
    val startPoint = airportConfig._2
    val endPoint = airportConfig._3

    val scPane = new ScrollPane
    scPane.prefWidth = stageW * 0.8
    scPane.prefHeight = stageH * 0.8

    val airportImg = new ImageView {
      image = new Image(this.getClass.getResourceAsStream("/img/" + lcCode + "_img.png"), stageW * 0.8, stageH * 0.8, true, true)
    }
    scPane.content = airportImg
    val imgScene = new Scene(scPane)
    val imgStage = new Stage
    val coordsTip = new Tooltip
    Tooltip.install(airportImg, coordsTip)
    val moveEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        val xRatio = event.x / airportImg.getImage.getWidth
        val yRatio = event.y / airportImg.getImage.getHeight
        val long = BigDecimal(startPoint._1 + ((endPoint._1 - startPoint._1) * xRatio)).setScale(4, BigDecimal.RoundingMode.HALF_UP)
        val lat = BigDecimal(startPoint._2 + ((endPoint._2 - startPoint._2) * yRatio)).setScale(4, BigDecimal.RoundingMode.HALF_UP)
        coordsTip.text = "(" + long + ", " + lat + ")"
        coordsTip.show(airportImg, event.x, event.y)
        event
      }
    }
    val clickEvent: (MouseEvent) => MouseEvent = { event: MouseEvent =>
      {
        val xRatio = event.x / airportImg.getImage.getWidth
        val yRatio = event.y / airportImg.getImage.getHeight
        val long = BigDecimal(startPoint._1 + ((endPoint._1 - startPoint._1) * xRatio)).setScale(4, BigDecimal.RoundingMode.HALF_UP)
        val lat = BigDecimal(startPoint._2 + ((endPoint._2 - startPoint._2) * yRatio)).setScale(4, BigDecimal.RoundingMode.HALF_UP)
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

  def onClearRun(id: String) {
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
      deletDir(new File("trained/model" + id))
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

  def onRefresh: Unit = {
    val buttons = getButtonSeq
    view.analysisBox.singleTestModule.modelSelector.items = ObservableBuffer(model.getModels)
    view.resultsBox.resultsPane.children = buttons
  }

  def getButtonSeq: Seq[AnchorPane] = {
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
        "output/runInfo" + runID + ".json")
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
          onClearRun(runID)
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

  def onTest(paramGrid: TilePane) {
    if (!model.modelMap.isEmpty) {
      view.analysisBox.singleTestModule.testLabel.text = "Predicting..."
      view.analysisBox.singleTestModule.testButton.disable = true
      view.analysisBox.singleTestModule.mapButton.disable = true
      val testParamArray = model.modelMap.head._2._2.featureExtracted.dropRight(1).map(feat => {
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

    } else println("model not loaded")
  }

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
        case Success(value) => {
          Platform.runLater {
            view.analysisBox.statusLabel.text = "Run Completed... standing by"
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

  def adjustTooltipDelay(tooltip: javafx.scene.control.Tooltip) = {
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