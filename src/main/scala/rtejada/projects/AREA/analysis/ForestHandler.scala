/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA.analysis

import org.apache.spark.sql._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import rtejada.projects.AREA.utils.FeatureWriter
import rtejada.projects.AREA.utils.Interface
import org.apache.spark.ml.PipelineStage

/**
 * Sets up pipeline for Random Forest model. Trains, tests and evaluates
 * for accuracy.
 */
class ForestHandler(data: DataFrame) {

  //Split data into training and testing sets
  val Array(trainingData, testingData) = data.randomSplit(Array(0.7, 0.3), 4873)

  //Indexing categorical features
  val stringColumns = data.drop("touchdownLong", "touchdownLat", "speed1", "speed2", "exit").columns
  val index_transformers: Array[PipelineStage] = stringColumns.map(
    colName => new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(s"${colName}Index")
      .fit(data)
      .setHandleInvalid("skip"))

  //Assemble input features to vector.
  val assembler = new VectorAssembler()
    .setInputCols(Array("runwayIndex", "depAirportIndex", "aircraftTypeIndex", "arrTerminalIndex",
      "arrGateIndex", "hourIndex", "dayIndex", "carrierIndex", "touchdownLong",
      "touchdownLat", "speed1", "speed2"))
    .setOutputCol("features")

  //Index label column
  val labelIndexer = new StringIndexer()
    .setInputCol("exit")
    .setOutputCol("label")
    .fit(trainingData).setHandleInvalid("skip")

  //Model Training
  val pipelineModel = trainModel(trainingData)

  //Model Testing
  val testResult = testModel(pipelineModel, testingData)

  //Output model details
  val rfModel = pipelineModel.stages(10).asInstanceOf[RandomForestClassificationModel]

  val forestDetails = assembleStringRF(rfModel)
  Interface.output(forestDetails, "randomForest.txt")

  //Feeds categorical and continuous features separately to the writer
  val featureWriter = new FeatureWriter(Array("runwayIndex", "depAirportIndex",
    "aircraftTypeIndex", "arrTerminalIndex", "arrGateIndex", "hourIndex", "dayIndex", "carrierIndex"),
    Array("touchdownLong", "touchdownLat", "speed1", "speed2"), index_transformers,
    labelIndexer)

  Interface.output(featureWriter.featureOutput, "features.json")

  //Results
  val predictions = testResult._1
  val accuracy = testResult._2

  private def trainModel(data: DataFrame): PipelineModel = {
    //Random Forest
    val classifierRF = new RandomForestClassifier()
      .setFeaturesCol(assembler.getOutputCol)
      .setImpurity("entropy")
      .setFeatureSubsetStrategy("sqrt")
      .setSeed(4283)
      .setMaxBins(2000)
      .setMaxDepth(7)
      .setNumTrees(105)

    //Predictions from index back to label
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedExit")
      .setLabels(labelIndexer.labels)

    //Pipeline chain feature transformers and random forest
    val pipeline = new Pipeline()
      .setStages(index_transformers ++
        Array(assembler, labelIndexer, classifierRF, labelConverter))

    pipeline.fit(data)
  }

  private def testModel(model: PipelineModel, data: DataFrame): (DataFrame, Double) = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction").setMetricName("accuracy")
    val predictions = model.transform(data)

    (predictions, evaluator.evaluate(predictions) * 100)
  }

  /**
   * Assembles the RF Model's node structure, overall forest feature importances,
   * as well as individual tree feature importances
   */
  private def assembleStringRF(randomForestModel: RandomForestClassificationModel): String = {

    val forestStructure = randomForestModel.toDebugString
    val forestImportances = randomForestModel.featureImportances.toArray.mkString(";")
    val treeImportances = randomForestModel.trees.reverse.map { x => x.featureImportances.toArray.mkString(";") }.mkString(System.lineSeparator())

    forestStructure +
      "TreeImportances" + System.lineSeparator() + treeImportances + System.lineSeparator() +
      "ForestImportances" + System.lineSeparator() + forestImportances
  }

}
