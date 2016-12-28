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
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.param.ParamMap
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

  //Categorical and Continuous feature names in separate arrays
  val namesCategorical = data.drop("exit").schema.fields.filter(struct => {
    struct.dataType.toString == "StringType"
  }).map(_.name)
  val namesContinuous = data.drop("exit").schema.fields.filter(struct => {
    struct.dataType.toString == "LongType" || struct.dataType.toString == "DoubleType"
  }).map(_.name)

  //Indexing categorical features
  val index_transformers: Array[PipelineStage] = namesCategorical.map(
    name => new StringIndexer()
      .setInputCol(name)
      .setOutputCol(s"${name}Index")
      .fit(data)
      .setHandleInvalid("skip"))

  //Assemble all features to vector.
  val catAssembler = new VectorAssembler()
    .setInputCols(namesCategorical.map(_ ++ "Index"))
    .setOutputCol("catFeatures")

  //Index label column
  val labelIndexer = new StringIndexer()
    .setInputCol("exit")
    .setOutputCol("label")
    .fit(trainingData).setHandleInvalid("skip")

  //Chi-squared selector, based on test of independence
  val catSelector = new ChiSqSelector()
    .setNumTopFeatures(5)
    .setFeaturesCol("catFeatures")
    .setLabelCol("label")
    .setOutputCol("selectedFeatures")

  //Assemble all categorical AND continuous features
  val finalAssembler = new VectorAssembler()
    .setInputCols("selectedFeatures" +: namesContinuous)
    .setOutputCol("features")

  //******
  //  Model Training & Testing
  //******
  val results = execute(trainingData, testingData)

  //Feeds categorical and continuous features separately to the writer
  val featureWriter = new FeatureWriter(namesCategorical, namesContinuous, index_transformers,
    labelIndexer)
  Interface.output(featureWriter.featureOutput, "features.json")

  //Results
  val predictions = results._1
  val accuracy = results._2
  val bestParams = results._3

  /**Executes training and testing of pipeline with set parameters*/
  private def execute(trainDF: DataFrame, testDF: DataFrame): (DataFrame, Double, String) = {
    //Random Forest
    val classifierRF = new RandomForestClassifier()
      .setFeaturesCol(finalAssembler.getOutputCol)
      .setImpurity("entropy")
      .setFeatureSubsetStrategy("sqrt")
      .setSeed(4283)
      .setSubsamplingRate(0.7)
      .setMaxBins(500)
      .setMaxDepth(3)
      .setNumTrees(20)

    //Predictions from index back to label
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedExit")
      .setLabels(labelIndexer.labels)

    //Pipeline chain feature transformers and random forest
    val pipeline = new Pipeline()
      .setStages(index_transformers ++
        Array(catAssembler, labelIndexer, catSelector, finalAssembler, classifierRF, labelConverter))

    val model = pipeline.fit(trainDF)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction").setMetricName("accuracy")
    val predictions = model.transform(testDF)

    //Output model details
    val optionModel = model.stages.find(transformer => transformer.toString.contains("RandomForest"))
    val forestDetails =
      if (optionModel.isDefined) assembleStringRF(optionModel.get.asInstanceOf[RandomForestClassificationModel])
      else "Random Forest Model not found in Pipeline"
    Interface.output(forestDetails, "randomForest.txt")

    (predictions, evaluator.evaluate(predictions) * 100, "No cross-validation")
  }
  
  /**Executes training and testing of pipeline with cross-validation*/
  private def executeTuning(trainDF: DataFrame, testDF: DataFrame): (DataFrame, Double, ParamMap) = {
    //Random Forest
    val classifierRF = new RandomForestClassifier()
      .setFeaturesCol(finalAssembler.getOutputCol)
      .setImpurity("entropy")
      .setFeatureSubsetStrategy("sqrt")
      .setSeed(4283)
      .setSubsamplingRate(0.7)
      .setMaxBins(500)

    //Predictions from index back to label
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedExit")
      .setLabels(labelIndexer.labels)

    //Pipeline chain feature transformers and random forest
    val pipeline = new Pipeline()
      .setStages(index_transformers ++
        Array(catAssembler, labelIndexer, catSelector, finalAssembler, classifierRF, labelConverter))

    //****Cross-validation
    def bestEstimatorParamMap(cvModel: CrossValidatorModel): ParamMap = {
      cvModel.getEstimatorParamMaps
        .zip(cvModel.avgMetrics)
        .maxBy(_._2)
        ._1
    }
    val nFolds: Int = 5

    //Grid search setup
    val paramGrid = new ParamGridBuilder().addGrid(classifierRF.numTrees, Array(118, 115, 122, 128))
      .addGrid(classifierRF.maxDepth, Array(7))
      .addGrid(catSelector.numTopFeatures, Array(5))
      .build()
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)
    val cvModel = cv.fit(trainDF)

    val predictions = cvModel.transform(testDF)

    (predictions, evaluator.evaluate(predictions) * 100, bestEstimatorParamMap(cvModel))
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
