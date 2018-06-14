package com.lgawron.spark.test

import com.lgawron.spark.test.WordsCount.WordCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StreamingContext, Duration}
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by lukasz.gawron on 14/06/2018.
  */
object WordsCountStreaming {
  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(ssc: StreamingContext,
            lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = count(ssc, lines, windowDuration, slideDuration, Set())(handler)

  def count(ssc: StreamingContext,
            lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration,
            stopWords: Set[String])
           (handler: WordHandler): Unit = {

    val sc = ssc.sparkContext
    val stopWordsVar = sc.broadcast(stopWords)

    val words = lines.transform(prepareWords(_, stopWordsVar))

    val wordCounts = words
      .map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration)
      .map {
        case (word: String, count: Int) => WordCount(word, count)
      }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

  private def prepareWords(lines: RDD[String], stopWords: Broadcast[Set[String]]): RDD[String] = {
    lines.flatMap(_.split("\\s"))
      .map(_.toLowerCase)
      .filter(!stopWords.value.contains(_)).filter(!_.isEmpty)
  }
}
