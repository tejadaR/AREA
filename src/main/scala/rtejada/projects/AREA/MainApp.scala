/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA

import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import rtejada.projects.AREA.model.AREAModel
import rtejada.projects.AREA.controller.OptionsController
import scalafx.scene.paint.Color
import scalafx.scene.control._
import scalafx.scene.control.TabPane.TabClosingPolicy
import rtejada.projects.AREA.view.OptionsView
import scalafx.stage.Screen

/** Primary stage, main model, view and controller initialized here */
class MainApp extends JFXApp {
  val stageX = Screen.primary.bounds.minX
  val stageY = Screen.primary.bounds.minY
  val stageWidth = Screen.primary.bounds.width
  val stageHeight = Screen.primary.bounds.height

  val mlModel: AREAModel = new AREAModel
  
  //Constructor-injection
  val optionsController: OptionsController = new OptionsController(mlModel, optionsView, stageWidth, stageHeight)
  val optionsView: OptionsView = new OptionsView(optionsController, stageWidth, stageHeight)

  stage = new PrimaryStage {
    title = "Airport Runway Exit Analysis"
    x = stageX
    y = stageY
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