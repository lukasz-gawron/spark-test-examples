package com.lgawron.spark.test

import org.apache.spark.rdd.RDD
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

import scala.collection.Map

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S01_IntegrationTest extends SparkSessionBase {
  val spark = ss
  it("should count words occurence in all lines") {
    Given("RDD of sentences")
    val linesRdd: RDD[String] = ss.sparkContext.parallelize(List("Ala ma kota", "Bolek i Lolek", "Ala ma psa"))

    When("extract and count words")
    val wordsCountRdd: RDD[(String, Int)] = WordsCount.extractFilterAndCountWords(linesRdd)
    val actual: Map[String, Int] = wordsCountRdd.collectAsMap()

    Then("words should be counted")
    val expected = Map(
      "Ala" -> 2,
      "Bolek" -> 1
    )
    actual should be(expected)
  }
}
