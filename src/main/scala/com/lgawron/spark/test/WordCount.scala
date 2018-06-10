package com.lgawron.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, DataFrame}
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

  def extractFilterAndCountWords(wordsDf: DataFrame): Dataset[(String, Long)] = {
    import wordsDf.sparkSession.implicits._
    wordsDf
      .flatMap((row: Row) => row.getAs[String]("value").split(" "))
      .where(
        col("value").equalTo("Ala")
        .or(col("value").equalTo("Bolek")))
      .map((word: String) => (word, 1))
      .groupByKey(wordOccurence => wordOccurence._1)
      .count()
  }

  def extractWords(line: String): Array[String] = {
    line.split(" ")
  }
}
