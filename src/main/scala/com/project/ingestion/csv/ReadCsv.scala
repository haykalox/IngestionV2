package com.project.ingestion.csv


import com.project.ingestion.connector._
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

    df.show(false)
    spark.sql("CREATE TABLE IF NOT EXISTS Data (QuotaAmount INT, StartDate DATE, Ownername STRING, Username STRING) ")

    df.write.mode("overwrite").saveAsTable("Data")

    spark.sql("SELECT * FROM Data").show()


  }
}
