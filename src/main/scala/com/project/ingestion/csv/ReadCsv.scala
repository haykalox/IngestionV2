package com.project.ingestion.csv

import java.lang

import com.project.ingestion.connector._
import javax.xml.bind.DataBindingException
import org.apache.commons.lang3.SystemUtils

object ReadCsv {
  def main(args: Array[String]): Unit = {

    // Create a Spark Session
    // For Windows
    if (SystemUtils.IS_OS_WINDOWS)
      System.setProperty("hadoop.home.dir", "C:\\winutils")


    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkConnector.getInstance().getSession()
    val test = JdbcConnector
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(args(0))
    println("Created Spark Session")

    df.show()
    df.write.format("csv").save(args(1))

    spark.sql("create external table testsql (Amount int , startdate date, OwnerName string, Username String )"
    +"ROW FORMAT DELIMITES FIELDS TERMINATED BY ';'" +
      "STORED AS CSV "
    +"Location '/data/' ");


  }
}
