package com.lgawron.spark.test

import com.lgawron.spark.test.WordsCount.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, Seconds}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, GivenWhenThen, FlatSpec}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Example from https://github.com/mkuthan/example-spark
  */
class S05_StreamingTestManualClock extends SparkStreamingBaseSpec with Eventually {

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5000, Millis)))
  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)

  it("Sample set should be counted") {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    val linesDStream: InputDStream[String] = ssc.queueStream(lines)
    WordsCountStreaming.count(ssc,
      linesDStream,
      windowDuration,
      slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
      results += wordsCount.collect()
    }

    ssc.start()

    When("first set of words queued")
    lines += ss.sparkContext.makeRDD(Seq("a", "b"))

    Then("words counted after first slide")
    advanceClock(slideDuration)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 1)))
    }
  }
}
