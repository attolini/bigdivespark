package com.attolini.bigdivespark

import org.apache.spark.sql.SparkSession

object Util extends java.io.Serializable {
  val months = Seq(
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  )

  def loadIMDBFiles(spark: SparkSession) = {
    val imdbRatings = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load(this.getClass.getResource("/IMDb_ratings.csv").getPath)
      .select("imdb_title_id", "weighted_average_vote")

    val imdbTitles = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load(this.getClass.getResource("/IMDb_movies.csv").getPath)
      .select("imdb_title_id", "title", "year", "genre")

    (imdbTitles, imdbRatings)
  }
}
