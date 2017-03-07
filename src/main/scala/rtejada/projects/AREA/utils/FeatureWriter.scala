/* This file is part of project AREA. 
 * See file LICENSE.md or go to github.com/tejadaR/AREA/blob/master/LICENSE.md for full license details. */

package rtejada.projects.AREA.utils

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.PipelineStage
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import java.io.File

/*** Writes the Random Forest's feature information to a JSON file*/
class FeatureWriter(catFeatNames: Array[String], contFeatNames: Array[String], indexedFeatures: Array[PipelineStage], labelIndexer: StringIndexerModel) {

  val catFeatures = formFeatures(catFeatNames, indexedFeatures, true)
  val contFeatures = formFeatures(contFeatNames, indexedFeatures, false)
  val featureOutput = formJson(catFeatures ++ contFeatures)

  /** Generates a JSON string from the given array of Features*/
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

  /** Forms feature information into a Feature class, and places the latter in an Array */
  private def formFeatures(names: Array[String], indexedFeats: Array[PipelineStage], categorical: Boolean): Array[Feature] = {
    val features = names.zipWithIndex.map {
      case (feat, i) =>
        val featName = feat.replace("Index", "")
        val featType = if (categorical) "categorical" else "continuous"
        if (categorical)
          Feature(featName, featType, Some(indexedFeats(i).asInstanceOf[StringIndexerModel].labels))
        else
          Feature(featName, featType, None)
    }
    features
  }
}

/** Represents a single feature and its information*/
case class Feature(featureName: String, featureType: String, categories: Option[Array[String]])