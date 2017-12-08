package com.adaltas.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    // val sconf: SparkConf() new SparkCont().setAppName("spark-example")
    // val sc = new SparkContext
    val sconf: SparkConf = new SparkConf().setAppName("spark-example")
    val sc: SparkContext = new SparkContext(sconf)
    println(s"APP-ID=${sc.applicationId}")

    val dataset = sc.textFile(s"hdfs://${args(0}")
    dataset.cache()

    val output = dataset
      .map(line => line.trim)
      .repartition(10)
      .map(line => ("value", line.split(",")))
      .filter(tuple => tuple._2(1) == "START")

    output.saveAsTextFile("spark_example_jar_output")

    sc.stop()
  }
}
