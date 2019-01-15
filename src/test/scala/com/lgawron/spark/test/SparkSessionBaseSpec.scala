package com.lgawron.spark.test

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll, FunSpec, Matchers}

/**
  * Created by lukasz.gawron on 10/06/2018.
  */
trait SparkSessionBaseSpec extends FunSpec with BeforeAndAfterAll with Matchers with GivenWhenThen {
  lazy val ss: SparkSession = {
    SparkSession
      .builder()
      .master("local[4]")
      .appName("spark test example")
      .config("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
      .config("spark.sql.streaming.checkpointLocation", s"/tmp/checkpoint_${System.currentTimeMillis()}")
      .getOrCreate()
  }
//  var ss: SparkSession = _
//
//  override def beforeAll() {
//
//    val conf = new SparkConf()
//      .setMaster("local[4]")
//      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
//
//    ss = SparkSession.builder()
//      .appName("TestApp" + System.currentTimeMillis())
//      .config(conf)
//      .enableHiveSupport()
//      .config("spark.sql.warehouse.dir", s"/tmp/test_${System.currentTimeMillis()}")
//      .config("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
//      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=/tmp/meta_${System.currentTimeMillis()};create=true")
//      .config("spark.sql.streaming.checkpointLocation", s"/tmp/checkpoint_${System.currentTimeMillis()}")
//      .config("spark.debug.maxToStringFields", 100)
//      .config(ConfVars.METASTOREURIS.varname, "")
//      .getOrCreate()
//
//  }
//
//  override def afterAll() {
//    ss.stop()
//    ss = null
//  }

  def getResourcePath(filePath: String): String = {
    getClass.getResource(filePath).getPath
  }
}
