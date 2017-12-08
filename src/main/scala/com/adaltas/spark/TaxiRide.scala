package com.adaltas.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * @author César Berezowski
  */
case class TaxiRide(
                     id: Long,
                     isStarted: Boolean,
                     startTime: Date,
                     endTime: Date,
                     startLong: Float,
                     startLat: Float,
                     endLong: Float,
                     endLat: Float,
                     passengerCount: Int
                   ) {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss")

  override def toString = s"TaxiRide(" +
    s"id=$id, " +
    s"isStarted=$isStarted, " +
    s"startTime=${dateFormat.format(startTime)}, " +
    s"endTime=${dateFormat.format(endTime)}, " +
    s"startLong=$startLong, +" +
    s"startLat=$startLat, " +
    s"endLong=$endLong, " +
    s"endLat=$endLat, " +
    s"passengerCount=$passengerCount" +
    s")"
}

object TaxiRide {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss")

  def apply(line: String) = {
    val values: Array[String] = line.split(",").map(_.trim)

    new TaxiRide(
      values(0).toLong,
      values(1).equals("STARTED"),
      dateFormat.parse(values(2)),
      dateFormat.parse(values(3)),
      values(4).toFloat,
      values(5).toFloat,
      values(6).toFloat,
      values(7).toFloat,
      values(8).toInt
    )
  }

  def row(line: String) = {
    val values: Array[String] = line.split(",").map(_.trim)

    Row(
      values(0).toLong,
      values(1).equals("STARTED"),
      new java.sql.Date(dateFormat.parse(values(2)).getTime),
      new java.sql.Date(dateFormat.parse(values(3)).getTime),
      values(4).toFloat,
      values(5).toFloat,
      values(6).toFloat,
      values(7).toFloat,
      values(8).toInt
    )
  }

  def schema = StructType(Seq(
    StructField("id", LongType),
    StructField("isStarted", BooleanType),
    StructField("startTime", DateType),
    StructField("endTime", DateType),
    StructField("startLong", FloatType),
    StructField("startLat", FloatType),
    StructField("endLong", FloatType),
    StructField("endLat", FloatType),
    StructField("passengerCount", IntegerType)
  ))
}
