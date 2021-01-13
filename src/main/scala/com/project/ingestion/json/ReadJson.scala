package com.project.ingestion.json

import com.project.ingestion.connector._


object ReadJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkConnector.getInstance().getSession()

    val df = spark.read
      .option("multiline","true")
      .json(args(0))

    df.show()
  }

}
