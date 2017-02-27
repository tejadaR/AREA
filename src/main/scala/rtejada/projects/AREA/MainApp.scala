package rtejada.projects.AREA

import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import rtejada.projects.AREA.model.MLModel
import rtejada.projects.AREA.controller.OptionsController
import scalafx.scene.paint.Color
import scalafx.scene.control._
import scalafx.scene.control.TabPane.TabClosingPolicy
import rtejada.projects.AREA.view.OptionsView

class MainApp extends JFXApp {
  val stageWidth = 1280D
  val stageHeight = 720D

  val mlModel: MLModel = new MLModel
  val optionsController: OptionsController = new OptionsController(mlModel, optionsView, stageWidth, stageHeight)
  val optionsView: OptionsView = new OptionsView(optionsController, stageWidth, stageHeight)

  stage = new PrimaryStage {
    title = "Airport Runway Exit Analysis"
    width = stageWidth
    height = stageHeight

    scene = new Scene {
      stylesheets = List(getClass.getResource("/css/General.css").toExternalForm)
      val rootPane = new TabPane
      rootPane.setTabClosingPolicy(TabClosingPolicy.AllTabs)

      val optionsTab = optionsView.tab
      optionsTab.closable = false

      rootPane.tabs = List(optionsTab)
      root = rootPane
    }

  }

}