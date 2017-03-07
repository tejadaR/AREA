/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.model

import scala.io.Source
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import scala.util.control.Breaks._
import rtejada.projects.AREA.utils.Interface

class ForestRun(featureFile: String, forestFile: String, runFile: String) {
  implicit val formats = net.liftweb.json.DefaultFormats

  //Loading files
  val featureJson = parse(Source.fromFile(featureFile).mkString)
  val runJson = parse(Source.fromFile(runFile).mkString)
  val featureExtracted = featureJson.extract[Array[Feature]]
  val runExtracted = runJson.extract[RunData]

  //Extracting methods for 'features' files
  def getFeatureName(index: Int) = featureExtracted(index).featureName
  def getFeatureType(index: Int) = featureExtracted(index).featureType
  def getFeatureCategories(index: Int) = featureExtracted(index).categories
  def getExitLabel(index: Int) = featureExtracted.last.categories.get(index)

  //Extracting methods for 'runInfo' files
  def getAirportCode = runExtracted.airportCode
  def getAccuracy = runExtracted.accuracy
  def getNumRunways = runExtracted.numRunways
  def getNumExits = runExtracted.numExits
  def getTrainCount = runExtracted.trainCount
  def getTestCount = runExtracted.testCount
  def getDuration = runExtracted.runDuration
  def getDate = runExtracted.date

  val lineArray = Source.fromFile(forestFile).getLines().toIndexedSeq.toArray
  val importanceLine = getImportancesLine(lineArray)
  val treeImportancesList = extractImportances(importanceLine, lineArray)
  val forestImportances = lineArray.last.split(";").map(_.toDouble)
  val maDepth = getMaxDepth(importanceLine, lineArray)
  val maxDepth = getMaxDepth(importanceLine, lineArray)

  val forest = assembleTrees(importanceLine, lineArray)
  def getForestNumTrees: Int = forest.length
  def getForestMaxDepth: Int = maxDepth

  /** Parses random forest output line by line, building node hierarchy for each tree */
  private def assembleTrees(endLine: Int, lineArray: Array[String]): List[TreeNode] = {
    var trees = List.empty[TreeNode]
    var previous: Option[TreeNode] = None
    for (i <- 1 until endLine) {
      val thisLine = lineArray(i)
      val nextLine = if (i != (lineArray.length - 1)) lineArray(i + 1) else ""

      if (thisLine.contains("Tree")) {
        //Root node creation
        val node = createNode(thisLine, nextLine, "root", previous)
        trees = trees :+ node
        previous = Some(node)

      } else if (!thisLine.contains("Predict")) {
        val depth = readDepth(thisLine)

        //Node creation
        val node = if (nextLine.contains("Predict")) createNode(thisLine, nextLine, "leaf", previous)
        else createNode(thisLine, nextLine, "normal", previous)

        //Adding node to its parent
        if (depth > previous.get.getDepth)
          previous.get.addChild(node)
        else if (depth == previous.get.getDepth)
          previous.get.getParent.asInstanceOf[TreeNode].addChild(node)
        else {
          while (depth < previous.get.getDepth)
            if (previous.get.getParent != None) previous = Some(previous.get.getParent.asInstanceOf[TreeNode])
          previous.get.getParent.asInstanceOf[TreeNode].addChild(node)
        }
        previous = Some(node)
      }
    }
    trees
  }

  /** Gets an list of the tree's assembled nodes */
  private def getTrees(importanceLine: Int, lineArray: Array[String]) = {
    val forestLines = lineArray.slice(1, importanceLine)
    val testArr = for (line <- forestLines) yield {

    }
  }

  /** Gets an list of the tree's assembled nodes */
  private def getMaxDepth(importanceLine: Int, lineArray: Array[String]) = {
    val forestLines = lineArray.slice(1, importanceLine).
      filter(line => !line.contains("Predict") && !line.contains("Tree"))
    val depthArray = forestLines.map(readDepth(_))
    //depthArray.foreach(println)
    depthArray.reduceLeft(_ max _)
  }

  /** Creates a node based on the type parameter (root, normal, or leaf node) */
  private def createNode(thisLine: String, nextLine: String, typeNode: String, previous: Option[TreeNode]): TreeNode = {
    val left = readLeft(thisLine)
    val featureIndex = if (typeNode != "leaf") readFeatureIndex(nextLine) else -1
    val condition = if (typeNode != "leaf") readCondition(nextLine, featureIndex) else " "

    val depth = if (typeNode != "root") readDepth(thisLine) else 0

    val node = typeNode match {
      case "root"   => new TreeNode(featureIndex, condition)
      case "normal" => new TreeNode(readDepth(thisLine), left, previous, featureIndex, condition)
      case "leaf" => {
        val prediction = readPrediction(nextLine)
        new TreeNode(readDepth(thisLine), left, previous, prediction)
      }
    }
    node
  }

  /** Extracts the every tree's feature importances as a List of Array[Double] */
  private def extractImportances(begin: Int, lineArray: Array[String]) = {
    val treeImpLines = lineArray.slice(begin, lineArray.length - 1).filter(!_.contains("Importances"))
    val treeImpArr = treeImpLines.map(_.split(";").map(_.toDouble).toArray).toList
    treeImpArr
  }

  /**
   * Gets the index of the first line containing the word "Importances".
   * If no line contains that word, returns the index of the first line with a space character
   */
  private def getImportancesLine(lineArray: Array[String]) = lineArray.indexOf(lineArray.find(_.contains("Importances")) match {
    case Some(line) => line
    case None       => " "
  })

  /** Gets the feature index from given line in the random forest output */
  private def readFeatureIndex(line: String): Int = {
    if (line.contains("If")) {
      line.substring(line.indexOf("If") + 12, line.indexOf("If") + 14)
        .replaceAll(" ", "")
        .toInt
    } else {
      line.substring(line.indexOf("Else") + 13, line.indexOf("Else") + 15)
        .replaceAll(" ", "")
        .toInt
    }
  }

  /** Gets the condition string from a given line in the random forest output */
  private def readCondition(line: String, featureIndex: Int): String = {
    val featureType = getFeatureType(featureIndex)
    if (featureType == "categorical") {
      if (!line.contains("not"))
        line.substring(line.indexOf("in"), line.indexOf("}") + 1)
      else
        line.substring(line.indexOf("not in"), line.indexOf("}") + 1)
    } else if (featureType == "continuous") {
      if (!line.contains("<="))
        line.substring(line.indexOf(">"), line.indexOf(")"))
      else {
        line.substring(line.indexOf("<="), line.indexOf(")"))
      }
    } else "No Condition"
  }

  /** If line contains the word "If", it is a left node. Otherwise it is a right node */
  private def readLeft(line: String) = if (line.contains("If")) true else false

  /** Gets node depth from a given line in the random forest output */
  private def readDepth(line: String): Int = {
    val charArray = line.toCharArray()
    if (charArray.length > 1) charArray.indexOf(charArray.find(_ != ' ').get) - 3 else {
      println("Error reading node depth")
      0
    }
  }

  /** Gets prediction index from line */
  private def readPrediction(line: String): Int = {
    val prediction = line.substring(line.indexOf("Predict:") + 8, line.length())
      .replaceAll(" ", "")
      .toDouble.toInt
    prediction
  }

  /** Gets nodes parameters: feature index, condition statement, and left from a line */
  private def getNodeParams(line: String): (Int, String, Boolean) = {
    val left = if (line.contains("If")) false else true
    val featureIndex: Int = if (line.contains("If")) {
      line.substring(line.indexOf("If") + 12, line.indexOf("If") + 14)
        .replaceAll(" ", "")
        .toInt
    } else {
      line.substring(line.indexOf("Else") + 13, line.indexOf("Else") + 15)
        .replaceAll(" ", "")
        .toInt
    }
    val featureType = getFeatureType(featureIndex)
    val condition = if (featureType == "categorical") {
      if (!line.contains("not")) line.substring(line.indexOf("in"), line.indexOf("}") + 1)
      else line.substring(line.indexOf("not in"), line.indexOf("}") + 1)
    } else {
      if (!line.contains("<=")) line.substring(line.indexOf("<="), line.indexOf(")"))
      else line.substring(line.indexOf(">"), line.indexOf(")"))
    }
    (featureIndex, condition, left)
  }
}

case class Feature(featureName: String, featureType: String, categories: Option[Array[String]])

case class RunData(airportCode: String, accuracy: Double, numRunways: Integer,
                   numExits: Integer, trainCount: Integer, testCount: Integer, runDuration: Integer, date: String)