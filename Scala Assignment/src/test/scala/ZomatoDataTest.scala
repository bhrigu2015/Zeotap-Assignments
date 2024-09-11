import org.scalatest.funsuite.AnyFunSuite

class ZomatoDataTest extends AnyFunSuite {
  val testDataPath = "./src/test/resources/TestData.csv"

  test("Data Loads Correctly") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    assert(csvParse.data.nonEmpty, "Data should not be empty after loading")
  }

  test("Parse Double Correctly") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    assert(csvParse.parseDouble("3.5") == 3.5, "Should parse '3.5' as 3.5")
    assert(csvParse.parseDouble("invalid") == 0.0, "Should return 0.0 for invalid input")
  }

  test("Top N Restaurants By Ratings") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val topRestaurants = csvParse.topNrestaurantsByRatings(3)
    assert(topRestaurants.length <= 3, "Should return at most 3 restaurants")
    assert(topRestaurants == topRestaurants.sortBy(-_._2), "Restaurants should be sorted by rating in descending order")
  }

  test("Top N Restaurants By Ratings Location Type") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val location = "TestLocation"
    val restaurantType = "TestType"
    val topRestaurants = csvParse.topNrestaurantsByRatingsLocationType(3, location, restaurantType)
    assert(topRestaurants.length <= 3, "Should return at most 3 restaurants")
    assert(topRestaurants == topRestaurants.sortBy(-_._2), "Restaurants should be sorted by rating in descending order")
  }

  test("Top N Restaurants By Rating Least Votes") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val location = "TestLocation"
    val topRestaurants = csvParse.topNrestaurantsByratingLeastVotes(3, location)
    assert(topRestaurants.length <= 3, "Should return at most 3 restaurants")
    assert(topRestaurants == topRestaurants.sortBy(r => (-r._2, r._3)), "Restaurants should be sorted by rating (desc) and then votes (asc)")
  }

  test("Number of Dishes Liked In Every Restaurant") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val dishesLiked = csvParse.noOfDishesLikedInEveryRestaurant()
    assert(dishesLiked.forall(_._2 >= 0), "Number of dishes liked should be non-negative")
    assert(dishesLiked == dishesLiked.sortBy(-_._2), "Results should be sorted by number of dishes in descending order")
  }

  test("Number of Distinct Locations") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val distinctLocations = csvParse.noOfDistinctLocations()
    assert(distinctLocations > 0, "There should be at least one distinct location")
    assert(distinctLocations <= csvParse.data.length, "Number of distinct locations should not exceed total number of restaurants")
  }

  test("Number of Distinct Cuisines At Location") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val location = "TestLocation"
    val distinctCuisines = csvParse.noOfDistinctCuisinesAtLocation(location)
    assert(distinctCuisines >= 0, "Number of distinct cuisines should be non-negative")
  }

  test("Number of Distinct Cuisines At Each Location") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val cuisinesByLocation = csvParse.noOfDistinctCuisinesAtEachLocation()
    assert(cuisinesByLocation.forall(_._2 >= 0), "Number of distinct cuisines should be non-negative for each location")
    assert(cuisinesByLocation.map(_._1).distinct.length == cuisinesByLocation.length, "Each location should appear only once")
  }

  test("Count of Restaurants For Each Cuisine") {
    val csvParse = new ZomatoData.CsvParse(testDataPath)
    csvParse.loadData()
    val restaurantsByCuisine = csvParse.countOfRestaurantsForEachCuisine()
    assert(restaurantsByCuisine.forall(_._2 > 0), "Count of restaurants should be positive for each cuisine")
    assert(restaurantsByCuisine.map(_._1).distinct.length == restaurantsByCuisine.length, "Each cuisine should appear only once")
  }
}
