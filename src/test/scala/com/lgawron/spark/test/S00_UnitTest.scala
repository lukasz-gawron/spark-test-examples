package com.lgawron.spark.test

import org.scalatest._

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S00_UnitTest extends FunSpec with BeforeAndAfterEach with Matchers with DiagrammedAssertions {

  it("should split a sentence into words") {
    val line = "Ala ma kota"

    val words: Array[String] = WordCount.extractWords(line = line)

    val expected = Array("Ala", "ma", "kota")
    words should be (expected)
  }
}
