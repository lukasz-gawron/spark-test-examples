package com.lgawron.spark.test

//import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.holdenkarau.spark.testing.{DatasetSuiteBase, StreamingSuiteBase}
import com.lgawron.spark.test.HelloWorld.{withFarewell, withGreeting}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{GivenWhenThen, FunSpec, FunSuite}

/**
  * Created by lukasz.gawron on 17/05/2018.
  */
class S07_Transform_FunctionsTest extends FunSpec with SparkSessionBaseSpec with GivenWhenThen with DatasetComparer {

  import ss.implicits._

  it("appends a greeting column to a Dataframe") {
    Given("Source dataframe")
    val sourceDF = Seq(
      ("Quality Excites")
    ).toDS()

    sourceDF.show()

    When("adding greeting column")
    val actualDF = sourceDF
      .transform(withGreeting())
//      .transform(withFarewell)
    actualDF.show()

    Then("new data frame contains column greeting")
    val expectedSchema = List(
      StructField("value", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("Quality Excites", "Hello!!")
    )

    val expectedDF = ss.createDataFrame(
      ss.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }
}


