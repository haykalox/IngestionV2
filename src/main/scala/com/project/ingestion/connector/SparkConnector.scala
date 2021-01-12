package com.project.ingestion.connector

import org.apache.spark.sql.SparkSession

class SparkConnector {
        def getSession() = {
            SparkSession
              .builder()
              .appName("Ingestion")
              .config("spark.master", "local")
              .enableHiveSupport()
              .getOrCreate()
        }
}

object SparkConnector {
  private var instance: SparkConnector = null
  def getInstance(): SparkConnector ={
    if (instance == null) {
      instance = new SparkConnector()
    }
    instance
  }
}