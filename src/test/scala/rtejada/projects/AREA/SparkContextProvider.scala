package rtejada.projects.AREA

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

trait SparkContextProvider {
  def sc: SparkContext
  def conf: SparkConf
}