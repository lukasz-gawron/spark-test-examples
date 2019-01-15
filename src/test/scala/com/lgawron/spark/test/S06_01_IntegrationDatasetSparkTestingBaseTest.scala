package com.lgawron.spark.test


import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.lgawron.spark.test.WordsCount.{Line, WordCount}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders._
import org.scalatest.{FunSpec, GivenWhenThen}

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S06_01_IntegrationDatasetSparkTestingBaseTest extends FunSpec with DatasetSuiteBase with GivenWhenThen {
  it("counting word occurences on few lines of text should return count Ala and Bolek words in this text") {
    Given("few lines of sentences")
    implicit val lineEncoder = product[Line]
    implicit val wordEncoder = product[WordCount]
    import spark.implicits._
    val linesDs = List(
      Line(text = "Ala ma kota"),
      Line(text = "Bolek i Lolek"),
      Line(text = "Ala ma psa")).toDS()

    When("extract and count words")
    val wordsCountDs: Dataset[WordCount] = WordsCount.extractFilterAndCountWordsDataset(linesDs)

    Then("filtered words should be counted")
    val expectedDs: Dataset[WordCount] = Seq(
      WordCount("Bolek", 1),
      WordCount("Ala", 2)
    ).toDS()
    assertDatasetEquals(expected = expectedDs, result = wordsCountDs)
  }
}


