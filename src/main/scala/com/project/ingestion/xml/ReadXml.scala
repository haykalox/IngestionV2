package com.project.ingestion.xml
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

object ReadXml {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = com.project.ingestion.connector.SparkConnector.getInstance().getSession()

    val df = spark.read
      .option("rootTag", "message")
      .option("rowTag", "personne")
      .xml("C:\\test\\test1.xml")

    df.show()


  }


  def toto(): Unit = {
    val spark: SparkSession = new com.project.ingestion.connector.SparkConnector().getSession()
  }

}
