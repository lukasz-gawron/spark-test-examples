package com.lgawron.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lukasz.gawron on 16/05/2018.
  */
object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Lecture")
    val sc = new SparkContext(conf)

    val words = List("Ala ma kota", "Bolek i Lolek", "Ala ma psa")

    val wordsRDD: RDD[String] = sc.parallelize(words)
    wordsRDD
      .flatMap(WordCount.extractWords)
      .map((word: String) => (word, 1))
      .reduceByKey((occurence1: Int, occurence2: Int) => {
        occurence1 + occurence2
      })
      .saveAsTextFile("/tmp/output")
  }


}
