import org.scalatest.funsuite.AnyFunSuite

class ZomatoDataTest  extends AnyFunSuite{

  test("Data Loads Correctly") {
    val csvParse = new ZomatoData.CsvParse("./src/test/resources/TestData.csv")
    csvParse.loadData()
    assert(csvParse.data.nonEmpty, "Data should not be empty after loading")
  }

  test("Parse Integer Correctly") {
    val csvParse = new ZomatoData.CsvParse("./src/test/resources/TestData.csv")
    assert(csvParse.parseInt("1,000") == 1000, "Should parse '1,000' as 1000")
    assert(csvParse.parseInt("invalid") == 0, "Should return 0 for invalid input")
  }

  test("Parse Double Correctly") {
    val csvParse = new ZomatoData.CsvParse("./src/test/resources/TestData.csv")
    assert(csvParse.parseDouble("3.5") == 3.5, "Should parse '3.5' as 3.5")
    assert(csvParse.parseDouble("invalid") == 0.0, "Should return 0.0 for invalid input")
  }

  test("Top N Restaurants By Ratings") {
    val csvParse = new ZomatoData.CsvParse("./src/test/resources/TestData.csv")
    csvParse.loadData()
    val topRestaurants = csvParse.topNrestaurantsByRatings(3)
    assert(topRestaurants.length == 3, "Should return exactly 3 restaurants")
    assert(topRestaurants == topRestaurants.sortBy(-_._2), "Restaurants should be sorted by rating in descending order")
  }

}