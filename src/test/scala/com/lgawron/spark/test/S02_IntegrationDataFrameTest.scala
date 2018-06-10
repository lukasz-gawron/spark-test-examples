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
  it("counting word occurences on few lines of text should return count of words Ala and Bolek in this text") {
    Given("few lines of sentences")
    val schema = StructType(List(
      StructField("value", StringType, true)
    )
    )
    val linesDf: DataFrame = ss.createDataFrame(
      ss.sparkContext.parallelize(Seq(
        Row("Ala ma kota"),
        Row("Bolek i Lolek"),
        Row("Ala ma psa")
      )),
      schema
    )

    When("extract and count words")
    val wordsCountDs: Dataset[(String, Long)] = WordCount.extractFilterAndCountWords(linesDf)
    val collected: Array[(String, Long)] = wordsCountDs.collect()
    val actual: Map[String, Long] = collected.toMap

    Then("filtered words should be counted")
    val expected = Map(
      "Ala" -> 2,
      "Bolek" -> 1
    )
    actual should be(expected)
  }
}
