package com.lendap.spark.lsh

/**
  * Created by maruf on 09/08/15.
  */

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkContext extends BeforeAndAfterAll {
  self: Suite =>
  @transient var session: SparkSession = _

  override def beforeAll() {
    session = SparkSession.builder().appName("test_lsh").master("local").getOrCreate()


    super.beforeAll()
  }

  override def afterAll() {
    if (session != null) {
      session.stop()
    }
    super.afterAll()
  }
}
