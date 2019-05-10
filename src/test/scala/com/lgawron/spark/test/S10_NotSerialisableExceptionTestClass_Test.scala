package com.lgawron.spark.test

import org.apache.spark.TaskContext
import org.apache.spark.sql.{AnalysisException, Dataset}

/**
  * Created by lukasz.gawron on 24/11/2018.
  */

class S10_NotSerialisableExceptionTestClass_Test extends SparkSessionBaseSpec {

  import ss.implicits._

  it("shows that case class defined on test class will have serialisation context including test class") {
    val thrown = intercept[AnalysisException] {
      val wordsDs: Dataset[NotSerialisableClass] = ss.createDataset(ss.sparkContext.parallelize(Seq(NotSerialisableClass("one"))))

      wordsDs.foreach(word => println(word.word))
    }
    assert(thrown != null)
  }

  case class NotSerialisableClass(word: String)

}
