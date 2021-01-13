package com.project.ingestion.xml
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

object ReadXml {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = com.project.ingestion.connector.SparkConnector.getInstance().getSession()

    val df = spark.read
      .option("rootTag", "defaultkeymap")
      .option("rowTag", "keymapgroup")
      .xml(args(0))

    df.show()


  }


}
