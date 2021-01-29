package com.classesTesting

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}

object Principal extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark example")
    .master("local[1]")
    .getOrCreate()

  val sqlContext = spark.sqlContext

  val responses = spark.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/RealEstate.csv")

  responses.printSchema()
  responses.show(false)

  val query =
    f"""
       |SELECT Price, Bedrooms, Bathrooms, Status
       |FROM
       |employee
       |WHERE Location LIKE 'Paso Robles' and Size > 2500
    """.stripMargin

  println("loading sql query")
  responses.createOrReplaceTempView("employee")
  //itworks ---=  spark.sql("SELECT Price FROM employee").show()
  spark.sql(query).show()





}
