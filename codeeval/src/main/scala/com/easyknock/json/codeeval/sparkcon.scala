package com.easyknock.json.codeeval

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object sparkcon {

  def sparkCon: SparkSession = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("EasyKnock JSON parsing Code challange")

      .getOrCreate()
    spark

  }

}