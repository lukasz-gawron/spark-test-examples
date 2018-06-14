package com.lgawron.spark.test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.lgawron.spark.test.WordsCount.{Line, WordCount}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders._

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S04_IntegrationDatasetFastTest extends SparkSessionBaseSpec with DatasetComparer {
  it("counting word occurences on few lines of text should return count Ala and Bolek words in this text") {
    Given("few lines of sentences")
    implicit val lineEncoder = product[Line]
    implicit val wordEncoder = product[WordCount]
    val lines = List(
      Line(text = "Ala ma kota"),
      Line(text = "Bolek i Lolek"),
      Line(text = "Ala ma psa"))
    val linesDs: Dataset[Line] = ss.createDataset(lines)

    When("extract and count words")
    val wordsCountDs: Dataset[WordCount] = WordsCount.extractFilterAndCountWordsDataset(linesDs)

    Then("filtered words should be counted")
    val expectedDs = ss.createDataset(Array(
      WordCount("Ala", 2),
      WordCount("Bolek", 1)
    ))
    assertSmallDatasetEquality(wordsCountDs, expectedDs, orderedComparison = false)
  }
}


