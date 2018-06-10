package com.lgawron.spark.test

import com.lgawron.spark.test.WordsCount.{Line, WordCount}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders._

import scala.collection.Map

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S03_IntegrationDatasetTest extends SparkSessionBase {
  it("counting word occurences on few lines of text should return count Ala and Bolek words in this text") {
    Given("few lines of sentences")
    implicit val lineEncoder = product[Line]
    val lines = List(
      Line(text = "Ala ma kota"),
      Line(text = "Bolek i Lolek"),
      Line(text = "Ala ma psa"))
    val linesDs: Dataset[Line] = ss.createDataset(lines)

    When("extract and count words")
    val wordsCountDf: Dataset[WordCount] = WordsCount.extractFilterAndCountWordsDataset(linesDs)
    val actualWordCount: Array[WordCount] = wordsCountDf.collect()

    Then("filtered words should be counted")
    val expectedWordCount = Array(
      WordCount("Ala", 2),
      WordCount("Bolek", 1)
    )
    actualWordCount should contain theSameElementsAs expectedWordCount
  }
}


