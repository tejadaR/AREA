/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.view

import scalafx.scene.layout._
import scalafx.scene.layout.AnchorPane
import scalafx.scene.Node
import scalafx.geometry.Pos

/**Basic GUI structure for a module with horizontal components */
trait TitledModuleH extends VBox  {
  val headerPane: AnchorPane = new AnchorPane
  val bodyPane: AnchorPane = new AnchorPane
  val bodyBox: HBox = new HBox
  bodyPane.children.add(bodyBox)
  this.children = List(headerPane, bodyPane)

}