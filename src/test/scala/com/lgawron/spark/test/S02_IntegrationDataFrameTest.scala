package com.lgawron.spark.test

import org.apache.spark
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, Dataset}
import org.scalatest.GivenWhenThen

import scala.collection.Map

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S02_IntegrationDataFrameTest extends SparkSessionBase {
  it("counting word occurences on few lines of text should return count Ala and Bolek words in this text") {
    Given("few lines of sentences")
    val schema = StructType(List(
      StructField("line", StringType, true)
    ))
    val linesDf: DataFrame = ss.read.schema(schema).json(getResourcePath("/text.json"))

    When("extract and count words")
    val wordsCountDf: DataFrame = WordCount.extractFilterAndCountWords(linesDf)
    val wordCount: Array[Row] = wordsCountDf.collect()

    Then("filtered words should be counted")
    val actualWordCount = wordCount
      .map((row: Row) =>
        Tuple2(row.getAs[String]("word"), row.getAs[Long]("count")))
      .toMap
    val expectedWordCount = Map(
      "Ala" -> 2,
      "Bolek" -> 1
    )
    actualWordCount should be(expectedWordCount)
  }
}
