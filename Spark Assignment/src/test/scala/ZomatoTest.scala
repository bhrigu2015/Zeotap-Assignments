import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class ZomatoTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("ZomatoDataTest")
    .master("local[*]")
    .getOrCreate()

  val csvParse = new ZomatoData.CsvParse("./src/test/resources/test_zomato_data.csv")

  test("CsvParse should load data correctly") {
    assert(csvParse.orignaldf.count() > 0)
    assert(csvParse.processedDf.columns.contains("numericRating"))
  }

  test("topNrestaurantsByRatings should return correct number of results") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.topNrestaurantsByRatings(5)
    }
    val output = outContent.toString
    assert(output.split("\n").length == 6)
  }

  test("topNrestaurantsByRatingsLocationType should filter correctly") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.topNrestaurantsByRatingsLocationType(3, "TestLocation", "TestType")
    }
    val output = outContent.toString
    assert(output.split("\n").length == 4)
    assert(output.contains("TestLocation"))
    assert(output.contains("TestType"))
  }

  test("topNrestaurantsByratingLeastVotes should order correctly") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.topNrestaurantsByratingLeastVotes(3, "TestLocation")
    }
    val output = outContent.toString
    assert(output.split("\n").length == 4)
    assert(output.contains("TestLocation"))
  }

  test("noOfDishesLikedInEveryRestaurant should count dishes correctly") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.noOfDishesLikedInEveryRestaurant()
    }
    val output = outContent.toString
    assert(output.contains("restaurantName") && output.contains("dishCount"))
  }

  test("noOfDistinctLocations should return correct count") {
    val distinctLocations = csvParse.noOfDistinctLocations()
    assert(distinctLocations > 0)
  }

  test("noOfDistinctCuisinesAtLocation should return correct count") {
    val location = "TestLocation"
    val distinctCuisines = csvParse.noOfDistinctCuisinesAtLocation(location)
    assert(distinctCuisines >= 0)
  }

  test("noOfDistinctCuisinesAtEachLocation should show results for each location") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.noOfDistinctCuisinesAtEachLocation()
    }
    val output = outContent.toString
    assert(output.contains("location") && output.contains("distinctCuisines"))
  }

  test("countOfRestaurantsForEachCuisine should show count for each cuisine") {
    val outContent = new java.io.ByteArrayOutputStream()
    Console.withOut(outContent) {
      csvParse.countOfRestaurantsForEachCuisine()
    }
    val output = outContent.toString
    assert(output.contains("cuisine") && output.contains("count"))
  }

  test("parseRating UDF should work correctly") {
    val testDf = spark.createDataFrame(Seq(("4.5/5",), ("3.2/5",))).toDF("rating")
    val result = testDf.withColumn("numericRating", csvParse.parseRating(col("rating")))

    val expectedResults = Array(4.5, 3.2)
    result.collect().zip(expectedResults).foreach { case (row, expected) =>
      assert(row.getAs[Double]("numericRating") === expected)
    }
  }

  spark.stop()

}