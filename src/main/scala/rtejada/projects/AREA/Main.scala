/*
 * Copyright (c) 2016 Roman Tejada. All rights reserved. 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * 
 * Contributors:
 * 	Roman Tejada - initial API and implementation
 */

package rtejada.projects.AREA

import org.apache.log4j.PropertyConfigurator
import rtejada.projects.AREA.utils.Interface
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.App
import scala.util._
import java.io.FileNotFoundException
import java.io.IOException
import javax.script.ScriptException
import java.util.Calendar

/** Entry point for project AREA.*/
object Main extends App {

  val mainApp = new MainApp().main(args)

}