package com.lgawron.spark.test

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.TaskContext
import org.apache.spark.sql.Dataset

/**
  * Created by lukasz.gawron on 24/11/2018.
  */
class S09_Cache_On_Executor_Test extends SparkSessionBaseSpec {

  import ss.implicits._

  it("test static class") {
    val wordsDs: Dataset[String] = List("one", "two", "three", "one").toDS()

    wordsDs.foreach(word => StaticCounter.counter += 1)

    println("Counter " + StaticCounter.counter)
  }

  it("test variable") {
    val wordsDs: Dataset[String] = List("one", "two", "three", "one").toDS()
    var counter = 0
    wordsDs
      .foreach(word => counter += 1)

    println("Counter " + counter)
  }

  it("shows that instance on threads will pass") {
    val wordsDs: Dataset[String] = List("one", "two", "three", "one", "five").toDS()

    val uniqueWordsCount: Long = countUniqueWords(wordsDs)
    println("No of words: " + uniqueWordsCount)
  }

  def countUniqueWords(wordsDs: Dataset[String]): Long = {
    wordsDs
      .filter(word => {
        val added = UniqueWordsCache.putIfAbsent(word)
        added
      })
      .count()
  }
}

object StaticCounter {
  var counter = 0
}
