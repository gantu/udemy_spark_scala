package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import scala.math.{min,max}
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


object MovieSimilarities{


	

	def main(args:Array[String]) = {
		val sc = new SparkContext("local[*]","MinTemp")
		val data = sc.textFile("/home/wild/projects/udemy_spark_scala/ml-100k/u.data")
		val nameDict = loadMovieNames("/home/wild/projects/udemy_spark_scala/ml-100k/u.item")
		val tupled = data.map(s=>s.split("\t")).map(s => (s(0),(s(1),s(2).toDouble)))
		val joinedRatings = tupled.join(tupled)
		val noDuplicates = joinedRatings.filter(filterDuplicateMovieRatings)
		val goodRatigs = noDuplicates.filter(filterSmallMovieRatings)
		val movieRatingPairs = goodRatigs.map(makeMovieRatingPairs)
		val groupedMovieRatingPairs = movieRatingPairs.groupByKey()
		val sims = groupedMovieRatingPairs.mapValues(computeCosineSimilarity)

		if (args.length > 0) {
      		val scoreThreshold = 0.97
      		val coOccurenceThreshold = 50.0
      
     		val movieID = args(0)
      
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     
      
      		val filteredResults = sims.filter( x =>
        	{
          		val pair = x._1
          		val sim = x._2
          		(pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        	}
      		)
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }

	}


	def filterDuplicateMovieRatings(movieRating:(String,((String,Double),(String,Double)))):Boolean={
		val movRate1 = movieRating._2._1
		val movRate2 = movieRating._2._2

		val mov1 = movRate1._1
		val mov2 = movRate2._1 
		mov2 < mov1
	}

	def filterSmallMovieRatings(movieRating:(String,((String,Double),(String,Double)))):Boolean={
		val movRate1 = movieRating._2._1
		val movRate2 = movieRating._2._2

		val mov1 = movRate1._2
		val mov2 = movRate2._2 
		(mov1 > 4.0) & (mov2 > 4.0)
	}


	def makeMovieRatingPairs(userMovieRating: (String,((String,Double),(String,Double)))):((String,String),(Double,Double))={
		((userMovieRating._2._1._1,userMovieRating._2._2._1),(userMovieRating._2._1._2,userMovieRating._2._2._2))
	}

	def computeCosineSimilarity(pairs:Iterable[(Double,Double)]):(Double,Int) = {
		val abSum = pairs.foldLeft(0)((a,b) => a + (b._1 * b._2).toInt)
		val aSquareSum = pairs.foldLeft(0)((a,b) => a + (b._1 * b._1).toInt)
		val bSquareSum = pairs.foldLeft(0)((a,b) => a + (b._2 * b._2).toInt)
		val denom = (Math.sqrt(aSquareSum).toDouble * Math.sqrt(bSquareSum).toDouble)
		val similarity = abSum.toDouble / denom

		(similarity,pairs.size)
	}

	def loadMovieNames(filename:String) : Map[String, String] = {
	    
	    // Handle character encoding issues:
	    implicit val codec = Codec("UTF-8")
	    codec.onMalformedInput(CodingErrorAction.REPLACE)
	    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
	    val bufferedSource = Source.fromFile(filename)
	    // Create a Map of Ints to Strings, and populate it from u.item.
	    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
	    bufferedSource.close
		val l = lines.map{line =>line.split('|') }
		val result = l map {x => (x(0) -> x(1))}
		val movieNames=result.toMap
	    movieNames
  }
}