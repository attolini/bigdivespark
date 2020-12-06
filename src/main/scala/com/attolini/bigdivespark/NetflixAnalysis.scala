package com.attolini.bigdivespark

import org.apache.spark.sql.SparkSession

object NetflixAnalysis extends App {
  def initSpark = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sqlC = spark.sqlContext
    spark.sparkContext.setLogLevel("ERROR")
    (spark, sqlC)
  }

  val (spark, sqlC) = initSpark

  import spark.implicits._
  import org.apache.spark.sql.functions._

  // Load Netflix file
  val netflixTitles = spark.read
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .load(this.getClass.getResource("/netflix_titles.csv").getPath)

  netflixTitles.printSchema
  netflixTitles.count

  /**
    * If a producer wants to release some content, which month must he chose?
    * it could be a Month when least amount of content is added
    */
  def extractMonth(s: String) = if (s == null) s else s.split(" ")(0)
  val extractMonthUDF = udf[String, String](extractMonth)

  val netflixWMonths =
    netflixTitles.withColumn("month", extractMonthUDF($"date_added"))
  val netflixWYears =
    netflixWMonths.withColumn("year", split($"date_added", " ")(2))

  val groupByMonthClean =
    netflixWYears
      .groupBy("month", "year")
      .agg(count("title").as("cont"))
      .filter($"month".isin(Util.months: _*))
      .orderBy(desc("year"))

  // months with less movies released on Netflix
  groupByMonthClean.filter("year = 2019").orderBy(asc("cont")).show

  /**
    * Analysing IMDB ratings to get top rated movies on Netflix, merging imdb data with netflix data
    */
  val (imdbTitles, imdbRatings) = Util.loadIMDBFiles(spark)
  // Now let's join the imdb titles with imdb ratings
  val ratings = imdbTitles
    .join(
      imdbRatings,
      imdbTitles("imdb_title_id") === imdbRatings("imdb_title_id"),
      "inner"
    )
    .select(
      imdbTitles("title").as("imdbTitle"),
      imdbTitles("year").as("imdbYear"),
      imdbRatings("weighted_average_vote").as("vote"),
      imdbTitles("genre")
    )

  /**
    * Let's perform inner join on the ratings dataset and netflix dataset
    * to get the content that has both ratings on IMDB and are available on Netflix
    */
  val netflixFilms = netflixTitles.where($"type".contains("Movie"))
  val netflixRated = netflixFilms
    .join(ratings, netflixFilms("title") === ratings("imdbTitle"), "inner")
    .orderBy(desc("vote"))
    .select(
      "type",
      "title",
      "director",
      "country",
      "date_added",
      "release_year",
      "duration",
      "description",
      "vote",
      "genre"
    )
  netflixRated.show

  // Calculate top rated movies in Italy
  netflixRated.where($"country".contains("Ita")).show

  /**
    * Calculate series with max number of seasons
    */
  val netflixShows =
    netflixTitles.where($"type".contains("TV Show")).select("title", "duration")
  import org.apache.spark.sql.types.IntegerType
  val netflixMaxDuration =
    netflixShows.withColumn(
      "n_seasons",
      regexp_replace($"duration", " Season(s?)", "").cast(IntegerType)
    )
  netflixMaxDuration.orderBy(desc("n_seasons")).show
}
