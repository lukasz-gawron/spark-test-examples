//package com.lgawron.spark.test
////
////import com.holdenkarau.spark.testing.RDDGenerator
////import org.scalacheck.Arbitrary
//import org.scalatest.FunSpec
//import org.scalatest.prop.{Checkers, TableDrivenPropertyChecks}
//
///**
//  * Created by lukasz.gawron on 17/06/2018.
//  */

//https://github.com/holdenk/spark-testing-base/blob/master/src/test/2.0/scala/com/holdenkarau/spark/testing/MLScalaCheckTest.scala
//class S06_PropertyBasedTesting extends SparkSessionBaseSpec with TableDrivenPropertyChecks with Checkers{
//  //  / A trivial property that the map doesn 't change the number of elements
//  it("map should not change number of elements") {
////    forAll(RDDGenerator.genRDD[String](ss.sparkContext, 10)(Arbitrary.arbitrary[String])){
////
////    }
////    val property =
////      forAll(RDDGenerator.genRDD[String](ss.sparkContext)(Arbitrary.arbitrary[String])) { rdd => rdd.map(_.length).count() == rdd.count() }
////    check(property)
////    check
//  }
//}
