package com.lgawron.spark.test

import com.lgawron.spark.test.WordsCount.{Line, WordCount}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders._

import scala.collection.Map

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S03_IntegrationDatasetTest extends SparkSessionBaseSpec{
  import ss.implicits._
  it("counting word occurences on few lines of text should return count Ala and Bolek words in this text") {
    Given("few lines of sentences")
    implicit val lineEncoder = product[Line]
    val linesDs = List(
      Line(text = "Ala ma kota"),
      Line(text = "Bolek i Lolek"),
      Line(text = "Ala ma psa")).toDS()

    When("extract and count words")
    val wordsCountDs: Dataset[WordCount] = WordsCount.extractFilterAndCountWordsDataset(linesDs)
    val actualWordCount: Array[WordCount] = wordsCountDs.collect()

    Then("filtered words should be counted")
    val expectedWordCount = Array(
      WordCount("Ala", 2),
      WordCount("Bolek", 1)
    )
    actualWordCount should contain theSameElementsAs expectedWordCount
  }
}


