package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

object tq2 {
  var joined:DataFrame=null
  var tTS:DataFrame=null
  var t2020:DataFrame=null
  /**
    * This is a "dummy" query which you can use as example code.
    */

  def deathBYcovid(): Unit =  {
    println("Dataframe read from CSV:")
    //var startTime = System.currentTimeMillis()
    tTS = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("time_series_covid_19_deaths_US.csv")
    //Converting related values into Double and Changing the Column Names
    tTS = tTS.withColumnRenamed("Province_state", "State")
      .withColumnRenamed("12/31/20", "Deaths")
      .withColumn("Deaths", col("Deaths").cast("double"))
    tTS = tTS.select("State","Deaths")
      .groupBy("State")
      .sum("Deaths")
      .orderBy("State")
    tTS=tTS.withColumnRenamed("sum(Deaths)","Covid_Deaths")
    tTS.show(3)


    t2020 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("usDeath2020.csv")
    t2020 = t2020.withColumnRenamed("State/Territory", "State")
      //.withColumn("Deaths", col("Deaths").cast("double"))
    .withColumn("Deaths", regexp_replace(col("Deaths"),",",""))
    t2020 = t2020.select(col("State"),col("Deaths").as("2020_Deaths"))
    t2020.show(3)

    joined = tTS.join(t2020, usingColumns = Seq("State"))
      .where(tTS.col("State")===t2020.col("State"))
    joined.show(3)







//    var transTime = (System.currentTimeMillis() - startTime) / 1000d
//    println(s"Transaction time: $transTime seconds")

  }
}
