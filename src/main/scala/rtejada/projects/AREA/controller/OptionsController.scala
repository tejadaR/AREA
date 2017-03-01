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

/** Controller implementation */
class OptionsController(mlModel: => MLModel, view: => OptionsView, width: Double, height: Double) {
  val model = mlModel

  def onRefreshResults: Seq[Button] = {
    val dir = new File("output")
    val fileSeq = if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toSeq else Seq[File]()

    for (file <- fileSeq if file.getName.contains("runInfo")) yield {
      val runID = file.getName.filter(_.isDigit)
      val forestRun = new ForestRun("output/features" + runID + ".json",
        "output/randomForest" + runID + ".txt",
        "output/runInfo" + runID + ".json")
      val resultsController: ResultsController = new ResultsController(model, forestRun)
      val resultsView: ResultsView = new ResultsView(resultsController, forestRun, width, height)
      new Button {
        text = "Run" + runID + System.lineSeparator() + forestRun.getAirportCode +
          ", Acc: " + BigDecimal(forestRun.getAccuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP) + "%"
        onAction = (ae: ActionEvent) => {
          if (!view.tab.getTabPane.getTabs.contains(resultsView.tab))
            view.tab.getTabPane.getTabs.add(resultsView.tab)
        }
      }
    }
  }

  def onTest(paramGrid: TilePane) {
    if (!model.modelMap.isEmpty) {
      view.analysisBox.singleTestModule.testLabel.text = "Predicting..."
      view.analysisBox.singleTestModule.testButton.disable = true
      val testParamArray = model.modelMap.head._2._2.map(feat => {
        if (feat.featureType == "categorical") {
          (feat.featureName, feat.featureType,
            view.analysisBox.singleTestModule.paramPane.children.toArray.
            map(_.asInstanceOf[javafx.scene.layout.HBox]).
            filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.ChoiceBox[String]]).
            map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.ChoiceBox[String]]).
            find(node => {
              node.getId == feat.featureName + "selector"
            }).get.getValue)
        } else
          (feat.featureName, feat.featureType,
            view.analysisBox.singleTestModule.paramPane.children.toArray.
            map(_.asInstanceOf[javafx.scene.layout.HBox]).
            filter(node => node.getChildren.get(1).isInstanceOf[javafx.scene.control.TextField]).
            map(_.getChildren.get(1).asInstanceOf[javafx.scene.control.TextField]).
            find(node => {
              node.getId == feat.featureName + "selector"
            }).get.getText)
      })

      val runFuture = Future { model.testSingleRecord(testParamArray) }
      runFuture.onComplete {
        case Success(prediction) => {
          Platform.runLater {
            view.analysisBox.singleTestModule.testButton.disable = false
            view.analysisBox.singleTestModule.testLabel.text = "Prediction: Exit " + prediction
          }
        }
        case Failure(e) => {
          Platform.runLater {
            view.analysisBox.singleTestModule.testButton.disable = false
            view.analysisBox.singleTestModule.testLabel.text = "testing failed"
            e.printStackTrace
          }
        }
      }

    } else println("model not loaded")
  }

  def onLoad(modelName: String): Unit = {
    view.analysisBox.singleTestModule.statusLabel.text = "Loading model..."
    view.analysisBox.singleTestModule.paramPane.children.clear
    view.analysisBox.singleTestModule.testButton.disable = true
    view.analysisBox.singleTestModule.loadButton.disable = true
    view.analysisBox.singleTestModule.testLabel.text = ""
    val runFuture = Future { model.loadModel(modelName, view) }
    runFuture.onComplete {
      case Success(featureArray) => {
        Platform.runLater {
          view.analysisBox.singleTestModule.statusLabel.text = modelName + " loaded"
          view.analysisBox.singleTestModule.testButton.disable = false
          view.analysisBox.singleTestModule.loadButton.disable = false
          val selectorArray = featureArray.map(feat => {
            val hBox = new HBox
            val label = new Label(formatFeature(feat.featureName))
            if (feat.featureType == "categorical") {
              val selector = new ChoiceBox[String] { id = feat.featureName + "selector" }
              selector.items = feat.categories match {
                case None             => ObservableBuffer.empty
                case Some(categories) => ObservableBuffer(categories.toSeq)
              }
              hBox.children.addAll(label, selector)
              hBox
            } else {
              val validDouble = Pattern.compile("-?((\\d*)|(\\d+\\.\\d*))")
              val converter = new DoubleStringConverter
              val selector = new TextField { id = feat.featureName + "selector" }
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

  def onRun(airport: String, treeNum: Int, depthNum: Int): Unit = {
    view.analysisBox.statusLabel.text = "Loading data..."
    view.analysisBox.runButton.disable = true
    view.analysisBox.runPb.visible = true
    view.resultsBox.refreshButton.disable = true
    val runFuture = Future { model.runModel(airport, treeNum, depthNum, view) }
    runFuture.onComplete {
      case Success(value) => {
        Platform.runLater {
          view.analysisBox.statusLabel.text = "Run Completed... standing by"
          view.analysisBox.runButton.disable = false
          view.analysisBox.runPb.progress = 0
          view.analysisBox.runPb.visible = false
          view.resultsBox.refreshButton.disable = false
        }
      }
      case Failure(e) => {
        Platform.runLater {
          view.analysisBox.statusLabel.text = e.getMessage //"Run Failed...Standing by"
          view.analysisBox.runButton.disable = false
          view.analysisBox.runPb.progress = 0
          view.analysisBox.runPb.visible = false
          view.resultsBox.refreshButton.disable = false
        }
        e.printStackTrace
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

}