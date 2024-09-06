import scala.io.{Source,StdIn}
import scala.collection.mutable.ArrayBuffer

object ZomatoData {

  case class Restaurant(
                       id: Int,
                       name: String,
                       rating: Double,
                       votes: Int,
                       location : String,
                       restaurant_type : String,
                       dishesLiked : List[String],
                       cuisines : List[String],
                       costForTwo: Int
                       )

  class CsvParse {
    val filePath = "../resources/zomato_cleaned - zomato_cleaned.csv"
    var data : ArrayBuffer[Restaurant]= ArrayBuffer()

    def loadData():Unit = {
      val source = Source.fromFile(filePath)
      try{
        for( line <- source.getLines()){
          val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim.replaceAll("^\"|\"$", ""))
          if(fields.length == 9){
            data +=Restaurant(
              id = fields(0).toInt,
              name = fields(1),
              rating = fields(2).split("/")(0).toDouble,
              votes = fields(3).toInt,
              location = fields(4),
              restaurant_type = fields(5),
              dishesLiked = fields(6).split(",").map(_.trim).toList,
              cuisines = fields(7).split(",").map(_.trim).toList,
              costForTwo = fields(8).toInt
            )
          }
        }
      }finally {
        source.close()
      }
    }

    def topNrestaurantsByRatings (n:Int): Seq[(String,Double)] = {
      data.sortBy(r => (-r.rating))
        .take(n)
        .map(r => (r.name,r.rating)).toSeq
    }

    def topNrestaurantsByRatingsLocationType (n:Int , location:String , restaurantType :String) : Seq[(String,Double)] = {
      data.filter(r => r.location== location && r.restaurant_type == restaurantType)
        .sortBy(r => (-r.rating))
        .take(n)
        .map(r => (r.name,r.rating))
        .toSeq
    }

    def topNrestaurantsByratingLeastVotes (n :Int , location : String) : Seq[(String,Double,Int)] = {
      data.filter(r=> r.location == location)
        .sortBy(r => (-r.rating,r.votes))
        .map(r => (r.name,r.rating,r.votes))
        .toSeq
    }

    def noOfDishesLikedInEveryRestaurant ():Seq[(String,Int)] = {
      data.map(r=> (r.name,r.dishesLiked.size))
        .sortBy(-_._2)
        .toSeq
    }

    def noOfDistinctLocations () : Int = {
      data.map(_.location)
        .distinct
        .size
    }

    def noOfDistinctCuisinesAtLocation (location : String) : Int = {
      data.filter(r=> r.location == location)
        .flatMap(_.cuisines)
        .distinct
        .size
    }

    def noOfDistinctCuisinesAtEachLocation () : Seq[(String,Int)] ={
      data.groupBy(_.location)
        .map{case (location,restaurants) =>
          (location,restaurants.flatMap(_.cuisines).distinct.size)
        }
        .toSeq
    }

    def countOfRestaurantsForEachCuisine(): Seq[(String, Int)] = {
      data.flatMap(_.cuisines)
        .groupBy(identity)
        .map { case (cuisine, occurrences) =>
          (cuisine, occurrences.size)
        }
        .toSeq
    }
  }

  def main(args: Array[String]): Unit = {

    val csv = new CsvParse()
    csv.loadData()

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

    val number : Int = StdIn.readLine().toInt

    val res = number match {
      case 1 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println(s"Top $n restaurants by rating:")
        println(csv.topNrestaurantsByRatings(n))
      case 2 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println("Enter Location:")
        val location :String = StdIn.readLine()
        println("Enter Restaurant Type:")
        val restaurantType: String = StdIn.readLine()
        println(s"Top $n restaurants by rating in a given location and restaurant type")
        println(csv.topNrestaurantsByRatingsLocationType(n,location,restaurantType))
      case 3 =>
        println("Enter N:")
        val n: Int = StdIn.readInt()
        println("Enter Location:")
        val location :String = StdIn.readLine()
        println(s"Top $n restaurants by rating and least number of votes in $location")
        println(csv.topNrestaurantsByratingLeastVotes(n,location))
      case 4 =>
        println("No. of dishes liked in every restaurant")
        println(csv.noOfDishesLikedInEveryRestaurant())
      case 5 =>
        println("No. of distinct locations")
        println(csv.noOfDistinctLocations())
      case 6 =>
        println("Enter Location:")
        val location :String = StdIn.readLine()
        println(s"No. of distinct cuisines at $location")
        println(csv.noOfDistinctCuisinesAtLocation(location))
      case 7 =>
        println("No. of distinct cuisines at each location")
        println(csv.noOfDistinctCuisinesAtEachLocation())
      case 8 =>
        println("Count of restaurants for each cuisine type")
        println(csv.countOfRestaurantsForEachCuisine())
      case _ => println("Enter a valid integer")
    }

  }

}
