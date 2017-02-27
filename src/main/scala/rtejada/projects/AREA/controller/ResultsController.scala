package rtejada.projects.AREA.controller

import rtejada.projects.AREA.model.MLModel
import rtejada.projects.AREA.view.ResultsView
import java.util.concurrent.TimeUnit

class ResultsController(mlModel: => MLModel) {
  val model = mlModel

  /** Gets a string with minutes and seconds from a millisecond input */
  def getMinSecStr(millis: Int): String = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(millis.toLong)
    val extraSeconds = TimeUnit.MILLISECONDS.toSeconds(millis.toLong) - (minutes * 60)
    minutes + "m " + extraSeconds + "s"
  }

}