package rtejada.projects.AREA.view

import scalafx.scene.layout._
import scalafx.scene.layout.AnchorPane
import scalafx.scene.Node
import scalafx.geometry.Pos

trait TitledModuleH extends VBox  {

  val headerPane: AnchorPane = new AnchorPane
  val bodyPane: AnchorPane = new AnchorPane
  val bodyBox: HBox = new HBox
  bodyPane.children.add(bodyBox)
  this.children = List(headerPane, bodyPane)

}