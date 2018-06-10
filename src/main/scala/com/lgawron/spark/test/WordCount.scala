package com.lgawron.spark.test

/**
  * Created by lukasz.gawron on 09/06/2018.
  */
object WordCount {
  def extractWords(line: String): Array[String] = {
    line.split(" ")
  }
}
