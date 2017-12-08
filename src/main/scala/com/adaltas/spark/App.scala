package com.adaltas.spark

import org.apache.spark.sql.hive.HiveContext
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

    val dataset = sc.textFile(s"hdfs://${args(0)}")
    dataset.cache()

    /*
    val output = dataset
      .map(line => line.trim)
      .repartition(10)
      .map(line => TaxiRide(line))
      .filter(ride => ride.isStarted)

    output.saveAsTextFile("spark_example_jar_output")
    */

    val hc = new HiveContext(sc)
    val df = hc.createDataFrame(dataset.map(line => TaxiRide.row(line)), TaxiRide.schema)
    df.show()

    df
      .write.saveAsTable("spark_example_output_df")

    sc.stop()
  }
}
