package com.lgawron.spark.test

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, Encoders, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by lukasz.gawron on 16/05/2018.
  */
object App {
  def main(args: Array[String]): Unit = {
    implicit val encoder = Encoders.product[SoccerEvent]
    var temp = Files.createTempDirectory("lgawron")
    println("temp path: " + temp.toAbsolutePath.toString)
    println("Hello World!")
    val conf = new SparkConf()
      .setMaster("local[4]")
    val ss = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .appName("Sport events")
      .getOrCreate()
    val schema = StructType(List(
      StructField("details", MapType(StringType, StringType, valueContainsNull = true)),
      StructField("event_type", LongType, nullable = true),
      StructField("id", LongType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("x", DoubleType, nullable = true),
      StructField("y", DoubleType, nullable = true)
    )
    )
    ss.read.schema(schema).json(ss.sparkContext.makeRDD(Seq(sportEvent)))
      .as[SoccerEvent]
      .filter(func = row => row.event_type == 16)
      .coalesce(1)
      .write.mode(SaveMode.Append).parquet(temp.toAbsolutePath.toString)
  }

  val sportEvent: String =
    """{
      |"id" : 1,
      |"event_type": 2,
      |"timestamp": "2018-05-13T12:00:05.000Z",
      |"x" : 56.0,
      |"y" : 45.0,
      |"details": {
      |   "speed":"1.0",
      |   "angle" : "2.0"
      | }
      |}
    """.stripMargin
  //  "event_qualifier_map": {
  //    56: "Back",
  //    141: "5.8",
  //    212: "22.9",
  //    140: "41.9",
  //    213: "5.9"
  //  },

  def newGreatStatistic(event: SoccerEvent): Double = {
    event.x * event.y
  }

  //  18/03/29 14:23:00 WARN utils.VerifiableProperties: Property max.partition.fetch.bytes is not valid
  case class Log(logLevel: String, appName: String, timestamp: String, className: String, entry: String)

  def read(): List[Seq[Int]] = {
    def lines = List(
      Seq(1, 2, 3),
      Seq(4, 5, 6),
      Seq(7, 8, 9)
    )
    lines
  }

  //  read().map(add)

}

case class SoccerEvent(id: BigInt, event_type: BigInt, timestamp: String, x: Double, y: Double, details: Map[String, Double])
