package com.lgawron.spark.test

import org.apache.spark.sql.{Row, Dataset, DataFrame}
import org.apache.spark.sql.functions._

object HelloWorld {

  def withGreeting()(df: Dataset[String]): Dataset[Row] = {
    df.withColumn("greeting", lit("Hello!!"))
  }

  val litFunction: () => String = () => "Hello!!"
  val udfLit = udf(litFunction)

  def withFarewell()(df: DataFrame): Dataset[Row] = {
    df.withColumn("farewell", lit("Goodbye!!"))
  }


}
