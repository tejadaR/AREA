package rtejada.projects.AREA

import org.apache.spark._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/**
 * Manages a local SparkContext variable,
 * ensuring it is stopped after each test.
 */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() {
    LocalSparkContext.stop(sc)
    sc = null
  }
}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) sc.stop()
  }

  /** Ensures SparkContext is stopped */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try
      f(sc)
    finally
      stop(sc)
  }
}