/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import rtejada.projects.AREA.analysis.Preprocessor
import rtejada.projects.AREA.analysis.ForestHandler

/** Handles preprocessing and machine learning stages. */
class ExitAnalysis(dataDF: DataFrame, configDF: DataFrame, airportCode: String) {

  //Pre-processing
  val preProcessor = new Preprocessor(dataDF, configDF, airportCode)
  val processedDF = preProcessor.finalDF.cache()
  
  //Random Forest Pipeline
  val forestHandler = new ForestHandler(processedDF)
}