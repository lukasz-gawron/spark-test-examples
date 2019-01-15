package com.lgawron.spark.test


import java.nio.file.Files

import org.apache.spark.streaming._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

/**
  * Created by lukasz.gawron on 10/06/2018.
  */
class SparkStreamingBaseSpec extends SparkSessionBaseSpec with BeforeAndAfterAll with Eventually {

  var ssc: TestStreamingContext = _
  def checkpointDir: String = Files.createTempDirectory(this.getClass.getSimpleName).toUri.toString
  def batchDuration: Duration = Seconds(1)

  override def beforeAll(): Unit = {
    super.beforeAll()
    ssc = new TestStreamingContext(ss.sparkContext, Milliseconds(1000))
//    ssc.checkpoint(checkpointDir)
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop(stopSparkContext = false, stopGracefully = false)
      ssc = null
    }

    super.afterAll()
  }

  def advanceClock(timeToAdd: Duration): Unit = {
    ClockWrapper.advance(ssc, timeToAdd)
  }

  def advanceClockOneBatch(): Unit = {
    advanceClock(Duration(batchDuration.milliseconds))
  }

}
