package com.lgawron.spark.test

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by lukasz.gawron on 21/11/2018.
  */
class S08_Serialisation_DatasetNotSerialised_Test extends SparkSessionBaseSpec {

  import ss.implicits._

  it("test") {
    val wordsDs: Dataset[String] = List("one", "two", "three", "one").toDS()
    val printer = new Printer(wordsDs)
    printer.print("two")
  }

  it("shows NPE after Dataset deserialisation on a executor thread") {
    val thrown = intercept[SparkException] {
      val wordsDs: Dataset[String] = List("one", "two", "three").toDS()

      val printer = new Printer(wordsDs)
      wordsDs
        .foreach(word => printer.print(word))
    }
    assert(thrown.getCause.isInstanceOf[NullPointerException])
  }

  it("shows NPE after Dataset deserialisation on method, not ctor") {
    val thrown = intercept[SparkException] {
      import ss.implicits._

      val wordsDs: Dataset[String] = ss.createDataset(Array("one", "two", "three"))
      // dataset on driver passed to constructor of NotSerializablePrinter,
      // later method from this class is passed in foreach
      // foreach will be executed on executor
      // so closure from print method is serialised including dataset
      val printer = new NotSerializablePrinter1()
      wordsDs.foreach(word => printer.print(word, wordsDs))
    }
    assert(thrown.getCause.isInstanceOf[NullPointerException])
  }
}

  class Printer(wordsDs: Dataset[String]) extends Serializable {
    def print(text: String) = {
      println("Word:" + text + " other words count:  " + wordsDs.filter(word => word != text).distinct().count())
    }
  }

  class NotSerializablePrinter1() extends Serializable {
    def print(msg: String, dsToPrint: Dataset[String]) =
      println(msg + "serialised df " + dsToPrint.rdd.getNumPartitions)
  }


