package netflix

object NetflixDataset extends App {


  /*
    Manual cleaning I did:
      show_id 5542 looks like raw data had 74 min in rating
      show_id s5795 duration in rating again
      s5814 as well Interesting all Louis Ck movies
   */

  val spark = SparkSession.builder()
    .appName("Netflix-dataset")
    .master("local")
    .getOrCreate()

  val schema = new StructType(
    fields = Array(
      StructField("show_id", StringType),
      StructField("type", StringType),
      StructField("title", StringType),
      StructField("director", StringType),
      StructField("cast", StringType),
      StructField("country", StringType),
      StructField("date_added", DateType),
      StructField("release_year", StringType),
      StructField("rating", StringType),
      StructField("duration", StringType),
      StructField("listed_in", StringType),
      StructField("description", StringType),
    )
  )


  val rawDF = spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("multiLine", "true")
    .option("dateFormat", "MMMM d, yyyy")
    .load("src/main/scala/resources/data.csv")


  //  rawDF.columns.foreach(colName => {
  //    val nullDF = rawDF.select("*").where(col(colName).isNull)
  //    println(s"Column: ${colName} has ${nullDF.count()} nulls")
  //    nullDF.show(false)
  //  })


  /*
  root
 |-- show_id: string (nullable = true)
 |-- type: string (nullable = true)
 |-- title: string (nullable = true)
 |-- director: string (nullable = true)
 |-- cast: string (nullable = true)
 |-- country: string (nullable = true)
 |-- date_added: date (nullable = true)
 |-- release_year: string (nullable = true)
 |-- rating: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- listed_in: string (nullable = true)
 |-- description: string (nullable = true)
 */


  val actorDF = rawDF.select("*")
    .withColumn("cast_array", split(col("cast"), ","))
    .withColumn("actor", explode(col("cast_array")))
    .drop("cast")
    .drop("cast_array")

  /*
    How many movies has an actor been in? And what movies?
    Actor: Count of movies they have been in
  */
  val actorMovieCount = actorDF
    .select("*")
    .groupBy("actor")
    .count()
    .orderBy(col("count").desc)
  // actorMovieCount.show()

  // What were the actual movies?
  val actorMovies = actorDF
    .select("*")
    .groupBy("actor")
    .agg(
      collect_set("title"),
      countDistinct("title").as("count")
    )
    .orderBy(col("count").desc)
  // actorMovies.show(false)


  /*
      What countries have an actor worked in?
   */

  val actorCountries = actorDF
    .select("*")
    .groupBy("actor")
    .agg(
      collect_set("country"),
      countDistinct("country").as("count")
    )
    .orderBy(col("count").desc)
  actorCountries.show(false)


  /*
    What is count of movies released per country?
   */
  val countryMovieCount = rawDF
    .select("*")
    .groupBy("country")
    .count().as("count")
    .orderBy(col("count").desc_nulls_last)
  countryMovieCount.show(false)


  /*
  What is count of movies released per director?
 */
  val directorMovieCount = rawDF
    .select("*")
    .groupBy("director")
    .count().as("count")
    .orderBy(col("count").desc_nulls_last)
  directorMovieCount.show(false)

  rawDF.select("*")
    .filter(col("type") === "Movie")
    .show(false)

  // What is the average run time?
  val runTimeDF = rawDF.select("*")
    .filter(col("type") === "Movie")
    .select(split(col("duration"), " ")(0).as("duration_min"))
    .agg(
      avg(col("duration_min")).as("avg_duration_min")
    )
  runTimeDF.show()

  // What is the date distribution?

}
