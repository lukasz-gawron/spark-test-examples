package com.lgawron.spark.test

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by lukasz.gawron on 25/11/2018.
  */
object LocalVsDistributedCache {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("spark test example")
      .getOrCreate()
    import ss.implicits._
    val wordsDs: Dataset[String] = ss.createDataset(Array("one", "two", "three", "one", "five"))

    val countUniqueWords: Long = LocalVsDistributedCache.countUniqueWords(wordsDs)
    println("No of words: " + countUniqueWords)
  }

  def countUniqueWords(wordsDs: Dataset[String]): Long = {
    wordsDs
      .filter(word => {
        val tc: TaskContext = TaskContext.get()
        println(s"Mitch: Word $word in partition " + tc.partitionId() + " processed in task " + tc.taskAttemptId())
        val added = UniqueWordsCache.putIfAbsent(word)
        added
      })
      .count()
  }
}


object UniqueWordsCache extends Serializable {

  val knownWords = ArrayBuffer[String]()

  def putIfAbsent(word: String): Boolean = {
    println("Mitch: Known words: ")
    knownWords.foreach(knownWord => println("Mitch: knownWord " + knownWord))
    if (!knownWords.contains(word)) {
      knownWords += word
      return true
    }
    false
  }
}
