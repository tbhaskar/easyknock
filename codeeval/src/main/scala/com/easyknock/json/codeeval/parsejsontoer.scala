package com.easyknock.json.codeeval

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object parsejsontoer {

  def main(args: Array[String]) {

    val spark = sparkcon.sparkCon

    import spark.implicits._

    val jsondf = spark.read.option("multiline", "true").json("C:\\work\\Jeremy\\sample_api_output_raw.json")
    jsondf.printSchema()

    val newdf = jsondf.withColumn("aschools", explode($"schoolDistrict.schools")).withColumn("afaculty", explode($"aschools.faculty")) // .show(truncate=false)

    println("Faculty Table")
    newdf.select("aschools.schoolName", "afaculty.name", "afaculty.role", "afaculty.startDate").distinct().show()
    
    println("School Table")
    newdf.select("schoolDistrict.districtId", "aschools.schoolGrade", "aschools.schoolName").distinct().show()

    println("address Table")
    newdf.select("address", "city", "medianHouseValue", "zip", "aschools.schoolName").distinct().show()

    /* another way to create DataFrame from JSON file
val df = spark.read.json(spark.sparkContext.wholeTextFiles("C:\\work\\Jeremy\\sample_api_output_raw.json").values)
df.printSchema()
df.show()
*
*/

    spark.stop()

  }

}