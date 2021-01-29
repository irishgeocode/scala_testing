package com.classesTesting

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._



object Principal extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //val session = SparkSession.builder().appName("sorting_average_price_per_SQ_Ft").master("local[1]").getOrCreate()

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark example")
    .master("local[1]")
    .getOrCreate()

  //val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  //val dataFrameReader = session.read
  val dataFrameReader = spark.read
  val responses = dataFrameReader
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/RealEstate.csv")

  //System.out.println("=== Print out schema ===")
  responses.printSchema()
  responses.show(false)

  val query =
    f"""
       |SELECT Price, Bedrooms, Bathrooms
       |FROM
       |employee
    """.stripMargin


  //session.sql(query).createOrReplaceTempView("viewMechanic")
  //viewMechanic.show(false)
  println("loading sql query")
  //session.sql(query).show()
  //session.sql.createDataFrame(query).show(false)
  //val resuls = sqlContext.sql(query)
  responses.createOrReplaceTempView("employee")
  //itworks ---=  spark.sql("SELECT Price FROM employee").show()
  spark.sql(query).show()





}
