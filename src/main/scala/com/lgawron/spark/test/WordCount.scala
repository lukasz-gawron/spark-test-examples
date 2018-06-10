package com.lgawron.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Created by lukasz.gawron on 09/06/2018.
  */
object WordCount {
  def extractFilterAndCountWords(wordsRDD: RDD[String]): RDD[(String, Int)] = {
    wordsRDD
      .flatMap(WordCount.extractWords)
      .filter(word => word == "Ala" || word == "Bolek")
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

  def extractWords(line: String): Array[String] = {
    line.split(" ")
  }
}
