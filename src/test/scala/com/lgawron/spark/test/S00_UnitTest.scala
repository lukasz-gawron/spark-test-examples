package com.lgawron.spark.test

import org.scalatest._

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S00_UnitTest extends FunSpec with Matchers {

  it("extracting words from single line of text, should extract words splitting by space") {
    val line = "Ala ma kota"

    val words: Array[String] = WordsCount.extractWords(line = line)

    val expected = Array("Ala", "ma", "kota")
    words should be (expected)
  }
}
