package rtejada.projects.AREA.view

import scalafx.scene.layout._
import scalafx.scene.layout.AnchorPane
import scalafx.scene.Node

trait TitledModuleV extends VBox {

  val headerPane: AnchorPane = new AnchorPane
  val bodyPane: AnchorPane = new AnchorPane
  val bodyBox: VBox = new VBox
  bodyPane.children.add(bodyBox)
  this.children = List(headerPane, bodyPane)

}