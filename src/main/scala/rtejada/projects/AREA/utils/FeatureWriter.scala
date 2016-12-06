/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA.utils

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.PipelineStage
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import java.io.File

/**
 * This class writes the Random Forest's feature information to a JSON file
 *
 * @author rtejada
 */
class FeatureWriter(featureNames: Array[String], indexedFeatures: Array[PipelineStage], labelIndexer: StringIndexerModel) {

  val featureOutput = formJson(formFeatures)

  /**
   * Generates a JSON string from the given array of Features
   */
  private def formJson(featureArray: Array[Feature]): String = {

    def assemble(inFeature: Feature): String = {
      val startStr = "{\"featureName\":\"" + inFeature.featureName +
        "\",\n\"featureType\":\"" + inFeature.featureType + "\",\n"
      val cat = inFeature.categories.getOrElse("None")
      if (inFeature.categories.getOrElse("None") != "None")
        startStr.concat("\"categories\":[\"" + cat.asInstanceOf[Array[String]].mkString("\",\"") + "\"]},\n")
      else
        startStr.concat("\"categories\":\"" + cat + "\"},\n")
    }

    "[" + featureArray.map(assemble(_)).mkString + "{\"featureName\":\"exit" +
      "\",\n\"featureType\":\"categorical\",\n" +
      "\"categories\":[\"" + labelIndexer.labels.mkString("\",\"") + "\"]}]"
  }

  /**
   * Forms feature information into a Feature
   * class, and places the latter in an Array
   */
  private def formFeatures(): Array[Feature] = {
    val features = featureNames.zipWithIndex.map {
      case (feat, i) =>
        def getFeatName(x: Int): String = x match {
          case 5 => "touchdownLong"
          case 8 => "speed1"
          case 9 => "speed2"
          case _ => featureNames(i).replace("Index", "")
        }
        def getFeatType(x: Int): String = x match {
          case 5 => "continuous"
          case 8 => "continuous"
          case 9 => "continuous"
          case _ => "categorical"
        }
        if (i == 5 || i == 8 || i == 9)
          Feature(getFeatName(i), getFeatType(i), None)
        else if (i < 5)
          Feature(getFeatName(i), getFeatType(i), Some(indexedFeatures(i).asInstanceOf[StringIndexerModel].labels))
        else
          Feature(getFeatName(i), getFeatType(i), Some(indexedFeatures(i - 1).asInstanceOf[StringIndexerModel].labels))
    }
    features
  }
}

/**
 * Represents a single feature and its information
 */
case class Feature(featureName: String, featureType: String, categories: Option[Array[String]])