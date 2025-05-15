package agh.wggios.analizadanych.datatransform

import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.functions._

class DataTransform extends SparkSessionProvider {

  def transform(df: DataFrame): DataFrame = {
    logInfo("transformuje dane")

    df.filter("some_column IS NOT NULL")
      .withColumn("new_column", upper(col("some_column")))
      .select("some_column", "new_column")
  }
}