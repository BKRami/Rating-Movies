import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object MainEntry {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val spark = SparkSession.builder().appName("myapp").master("local[*]").getOrCreate()
    val USER_FILE_PATH = "src/resource/users.dat"
    val MOVIE_FILE_PATH = "src/resource/movies.dat"
    val RATING_FILE_PATH = "src/resource/ratings.dat"

    //read as text
    val users = spark.sparkContext.textFile(USER_FILE_PATH)
    val movies = spark.sparkContext.textFile(MOVIE_FILE_PATH)
    val ratings = spark.sparkContext.textFile(RATING_FILE_PATH)

    //map to case class
    val rddUsers = users.map(mapToUsers)
    val rddMovies = movies.map(mapToMovies)
    val rddRatings = ratings.map(mapToRatings)

    //convert rdd to dataframe
    val dfUserWithSchema = spark.createDataFrame(rddUsers).toDF("userId", "gender","age","occupation","zipcode")
    val dfMoviesWithSchema = spark.createDataFrame(rddMovies).toDF("movieId", "title","year","genre")
    val dfRatingsWithSchema = spark.createDataFrame(rddRatings).toDF("userId", "movieId","rating","timestamp")

    /*
    Please calculate the average ratings, per genre and year (by year we mean the year in which the movies were released).
    Please consider only movies, which were released after 1989.
    Please consider the ratings of all persons aged 18-49 years
    */

    //create TempViews
    dfMoviesWithSchema.createOrReplaceTempView("movies")
    dfRatingsWithSchema.createOrReplaceTempView("ratings")
    dfUserWithSchema.createOrReplaceTempView("user")

  // Trying to use SparkSQL but missing a part where to deal with each genre (equivalent of explode in SQL
    //To show the time difference between the two solutions

   /*
   val resultSQLDF=spark.sql("select avg(rating),count(movies.movieId),year,genre from movies " +
      " inner join ratings " +
      " ON movies.movieId=ratings.movieId " +
      " inner join user " +
      " ON user.userId=ratings.userId " +
      " where year>1989 and age >1 and age <50 " +
      " group by year,genre" )
      */

    //resultDF.show(4)

    val gfkDF=dfMoviesWithSchema.join(dfRatingsWithSchema,"movieId").join(dfUserWithSchema,"userId")
      .filter(dfUserWithSchema("age")<50 && dfUserWithSchema("age")>1)
      .filter(dfMoviesWithSchema("year")>1989)
      .select(explode(dfMoviesWithSchema("genre")),dfRatingsWithSchema("rating"),dfMoviesWithSchema("year"))
      .withColumnRenamed("col","genres")
      .groupBy("genres","year")
      .avg("rating")
      .orderBy("genres","year")


    val result=gfkDF
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ";")
      .save("src/resource/results")


    spark.stop()

  }
  def mapToUsers(line : String) : User = {
    var splitted = line.split("::")
    User(splitted(0),splitted(1),splitted(2).toInt,splitted(3),splitted(4))
  }
  def mapToMovies(line : String) : Movie = {
    var splitted = line.split("::")
    var year=splitted(1).split("[\\(\\)]")

    var genres=splitted(2).split("\\|")

    Movie(splitted(0),splitted(1),year(year.length-1),genres)
  }
  def mapToRatings(line : String) : Rating = {
    var splitted = line.split("::")
    Rating(splitted(0),splitted(1),splitted(2).toDouble,splitted(3))
  }

  case class User(id : String,gender : String,age:Int,occupation:String,zipCode : String)
  case class Movie(movieID : String,title : String, year : String ,genre : Array[String])
  case class Rating(userID: String,movieID:String,rating: Double,timestamp: String)
  case class MoviePerYear(movieID: String, Year: Int)
  case class MovieGenres(movieID:String, genre: Array[String])

}
