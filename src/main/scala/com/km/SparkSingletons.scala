package com.km

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sdhandapani on 1/28/2017.
  */
object SparkSingletons {
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
}
