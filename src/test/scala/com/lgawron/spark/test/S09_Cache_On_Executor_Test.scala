package com.lgawron.spark.test

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.TaskContext
import org.apache.spark.sql.Dataset

/**
  * Created by lukasz.gawron on 24/11/2018.
  */
class S09_Cache_On_Executor_Test extends SparkSessionBaseSpec {

  import ss.implicits._

  /**
    * .master("local[4]") 4 threads
    * Local mode - unpredictable results due to as increment is not atomic operation,
    * happening on shared static variable in the same JVM, may be improved e.g. byt AtomicInteger to be deterministic
    * Results:
    *   Counter 3
    *   Counter 4
    *
    * Cluster mode - always 0, as executors will have separate JVM from driver.
    * Driver variable won't be incremented, as incrementing happen only on executors.
    *  Counter 0
    *  Counter 0
    */
  it("will have unpredictable value of counter, as increment is not atomic operation, " +
    "happening on shared static variable") {
    val wordsDs: Dataset[String] = List("one", "two", "three", "one").toDS()

    wordsDs.foreach(word => StaticCounter.counter += 1)

    println("Counter " + StaticCounter.counter)
  }

  /**
    * Standard example from Spark documentation
    * https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-
    */
  it("should confirm Spark documentation example") {
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
