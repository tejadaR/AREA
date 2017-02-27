package rtejada.projects.AREA.model

/**
 * Implementation class of a decision tree by sub-classing
 * If this node does not have a prediction, input -1
 * @author rtejada
 */
class TreeNode(depth: Int, left: Boolean, parent: Option[TreeNode], featureIndex: Int, condition: String, prediction: Int) {

  //Root Node constructor
  def this(featureIndex: Int, condition: String) = this(0, false, None, featureIndex, condition, -1)

  //Normal Node constructor
  def this(depth: Int, left: Boolean, parent: Option[TreeNode], featureIndex: Int, condition: String) = this(depth, left, parent, featureIndex, condition, -1)

  //Leaf Node constructor
  def this(depth: Int, left: Boolean, parent: Option[TreeNode], prediction: Int) = this(depth, left, parent, -1, " ", prediction)

  var childNodes: List[TreeNode] = List.empty[TreeNode]  
  var gridPosX = 0

  def getDepth = depth
  def getFeatureIndex = featureIndex
  def getleft = left
  def getCondition = condition
  def getChildren = childNodes
  def getPrediction = prediction
  def getGridPosX = gridPosX

  def setGridPosX(x: Int) = { gridPosX = x }

  /** Adds a child node  */
  def addChild(child: TreeNode) = {
    childNodes = child :: childNodes
  }

  /** Retrieves a child node by type ('if' or 'else') */
  def getChild(left: Boolean): Option[TreeNode] = {
    if (childNodes.isEmpty)
      None
    else {
      if (childNodes(0).getleft == left)
        Some(childNodes(0))
      else
        Some(childNodes(1))
    }
  }

  /** Retrieves parent node */
  def getParent() = parent match {
    case Some(node) => node
    case None       => None
  }

  def getSide: String = {
    if (this.getDepth > 1)
      parent.get.getSide
    else if (this.left) "left" else "right"
  }

}