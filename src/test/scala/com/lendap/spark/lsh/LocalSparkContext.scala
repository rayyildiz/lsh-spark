package com.lendap.spark.lsh

/**
  * Created by maruf on 09/08/15.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkContext extends BeforeAndAfterAll {
  self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    System.setProperty("hadoop.home.dir", "D:\\Apps\\BigData\\hadoop\\hadoop-2.9.0\\")

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}
