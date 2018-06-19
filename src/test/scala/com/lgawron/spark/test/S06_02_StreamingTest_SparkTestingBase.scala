package com.lgawron.spark.test

//import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.holdenkarau.spark.testing.{StreamingSuiteBase, DatasetSuiteBase}
import com.lgawron.spark.test.WordsCount.{Line, WordCount}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{FunSuite, FunSpec, GivenWhenThen}

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S06_02_StreamingTest_SparkTestingBase extends FunSuite with StreamingSuiteBase {

  test("count words") {
    val input = List(List("a b"))
    val expected = List(List(("a", 1), ("b", 1)))
    testOperation[String, (String, Int)](input, count _, expected, ordered = false)
  }

  // This is the sample operation we are testing
  def count(lines: DStream[String]): DStream[(String, Int)] = {
    lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  test("CountByWindow with windowDuration 3s and slideDuration=2s") {
    // There should be 2 windows :  {batch2, batch1},  {batch4, batch3, batch2}
    val batch1 = List("a", "b")
    val batch2 = List("d", "f", "a")
    val batch3 = List("f", "g"," h")
    val batch4 = List("a")
    val input= List(batch1, batch2, batch3, batch4)
    val expected = List(List(5L), List(7L))

    def countByWindow(ds:DStream[String]):DStream[Long] = {
      ds.countByWindow(windowDuration = Seconds(3), slideDuration = Seconds(2))
    }

    testOperation[String, Long](input, countByWindow _, expected, ordered = true)
  }

}


