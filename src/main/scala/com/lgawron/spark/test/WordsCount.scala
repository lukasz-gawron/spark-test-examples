package com.lgawron.spark.test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Time, StreamingContext, Duration}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by lukasz.gawron on 09/06/2018.
  */
object WordsCount {
  def extractAndCountWords(wordsRDD: RDD[String]): RDD[(String, Int)] = {
    wordsRDD
      .flatMap(extractWords)
      .map((word: String) => (word, 1))
      .reduceByKey((occurence1: Int, occurence2: Int) => {
        occurence1 + occurence2
      })
  }

  def extractFilterAndCountWords(wordsDf: DataFrame): DataFrame = {
    val words: Column = explode(split(col("line"), " ")).as("word")
    wordsDf
      .select(words)
      .where(
        col("word").equalTo("Ala").or(col("word").equalTo("Bolek")))
      .groupBy("word")
      .count()
  }

  case class Line(text: String)

  case class WordCount(word: String, count: Long)

  def extractFilterAndCountWordsDataset(wordsDs: Dataset[Line]): Dataset[WordCount] = {
    import wordsDs.sparkSession.implicits._
    wordsDs
      .flatMap((line: Line) => line.text.split(" "))
      .filter((word: String) => word == "Ala" || word == "Bolek")
      .withColumnRenamed("value", "word")
      .groupBy(col("word"))
      .agg(count("word").as("count"))
      .as[WordCount]
  }

  def extractWords(line: String): Array[String] = {
    line.split(" ")
  }



}
