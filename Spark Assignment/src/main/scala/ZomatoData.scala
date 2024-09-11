import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.StdIn

object ZomatoData {

  class CsvParse(filePath: String = "./src/main/resources/zomato_cleaned.csv") {

    val spark = SparkSession.builder()
      .appName("Zomato_data")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("restaurantName", StringType, nullable = false),
      StructField("rating", StringType, nullable = true),
      StructField("numberOfVotes", IntegerType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("restaurantType", StringType, nullable = true),
      StructField("dishesLiked", StringType, nullable = true),
      StructField("typesOfCuisines", StringType, nullable = true),
      StructField("costForTwo", IntegerType, nullable = true)
    ))

    val orignaldf: DataFrame = spark.read
      .option("header", value = false)
      .schema(schema)
      .csv(filePath)

    val parseRating = udf((ratingStr: String) => {
      ratingStr.split("/")(0).toDouble
    })

    val processedDf = orignaldf.withColumn("numericRating", parseRating(col("rating")))

    def topNrestaurantsByRatings(n: Int)  = {
      processedDf.orderBy(col("numericRating").desc, col("numberOfVotes").desc)
        .select("restaurantName", "rating", "numberOfVotes", "location", "typesOfCuisines", "costForTwo")
        .show(n, truncate = false)
    }

    def topNrestaurantsByRatingsLocationType(n: Int, location: String, restaurantType: String): Unit = {
      processedDf.filter(col("location") === location && col("restaurantType") === restaurantType)
        .orderBy(col("numericRating").desc, col("numberOfVotes").desc)
        .select("restaurantName", "rating", "numberOfVotes", "location", "typesOfCuisines", "costForTwo")
        .show(n, truncate = false)
    }

    def topNrestaurantsByratingLeastVotes(n: Int, location: String): Unit = {
      processedDf.filter(col("location") === location)
        .orderBy(col("numericRating").desc, col("numberOfVotes").asc)
        .select("restaurantName", "rating", "numberOfVotes", "location", "typesOfCuisines", "costForTwo")
        .show(n, truncate = false)
    }

    def noOfDishesLikedInEveryRestaurant(): Unit = {
      processedDf.withColumn("dishCount", size(split(col("dishesLiked"), ",")))
        .select("restaurantName", "dishCount")
        .orderBy(col("dishCount").desc)
        .show(truncate = false)
    }

    def noOfDistinctLocations(): Long = {
      processedDf.select(col("location")).distinct().count()
    }

    def noOfDistinctCuisinesAtLocation(location: String): Long = {
      processedDf.filter(col("location") === location)
        .select(explode(split(col("typesOfCuisines"), ",")).as("cuisine"))
        .distinct()
        .count()
    }

    def noOfDistinctCuisinesAtEachLocation(): Unit = {
      processedDf.select(col("location"), explode(split(col("typesOfCuisines"), ",")).as("cuisine"))
      .groupBy("location")
      .agg(countDistinct("cuisine").as("distinctCuisines"))
      .orderBy(col("distinctCuisines").desc)
      .show(truncate = false)
    }

    def countOfRestaurantsForEachCuisine(): Unit = {
      processedDf.select(explode(split(col("typesOfCuisines"),",")).as("cuisines"))
        .groupBy("cuisine")
        .count()
        .orderBy(col("count").desc)
        .show(truncate = false)
    }
  }

  def main(args: Array[String]): Unit = {

    val csv = new CsvParse()

    println("Choose a Function to Perform:")
    println("1: Top N restaurants by rating")
    println("2: Top N restaurants by rating in a given location and restaurant type")
    println("3: Top N restaurants by rating and least number of votes in a given location")
    println("4: No. of dishes liked in every restaurant")
    println("5: No. of distinct locations")
    println("6: No. of distinct cuisines at a certain location")
    println("7: No. of distinct cuisines at each location")
    println("8: Count of restaurants for each cuisine type")
    println("Enter number corresponding to the Function:")

    val number: Int = StdIn.readLine().toInt

    number match {
      case 1 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println(s"Top $n restaurants by rating:")
        csv.topNrestaurantsByRatings(n)
      case 2 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println("Enter Location:")
        val location: String = StdIn.readLine()
        println("Enter Restaurant Type:")
        val restaurantType: String = StdIn.readLine()
        println(s"Top $n restaurants by rating in $location and $restaurantType restaurant")
        csv.topNrestaurantsByRatingsLocationType(n, location, restaurantType)
      case 3 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println("Enter Location:")
        val location: String = StdIn.readLine()
        println(s"Top $n restaurants by rating and least number of votes in $location")
        csv.topNrestaurantsByratingLeastVotes(n, location)
      case 4 =>
        println("No. of dishes liked in every restaurant")
        csv.noOfDishesLikedInEveryRestaurant()
      case 5 =>
        println("No. of distinct locations")
        println(csv.noOfDistinctLocations())
      case 6 =>
        println("Enter Location:")
        val location: String = StdIn.readLine()
        println(s"No. of distinct cuisines at $location")
        println(csv.noOfDistinctCuisinesAtLocation(location))
      case 7 =>
        println("No. of distinct cuisines at each location")
        csv.noOfDistinctCuisinesAtEachLocation()
      case 8 =>
        println("Count of restaurants for each cuisine type")
        csv.countOfRestaurantsForEachCuisine()
      case _ => println("Enter a valid integer")
    }

    csv.spark.stop()
  }
}
