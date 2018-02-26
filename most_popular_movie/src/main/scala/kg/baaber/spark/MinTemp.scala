package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import scala.math.{min,max}
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostPopularMovie{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","MinTemp")
		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/ml-100k/u.data")
		val nameDict = sc.broadcast(loadMovieNames("/home/wild/projects/udemy_spark_scala/ml-100k/u.item"))
		val rdd = lines.map(parseLines)
		val moviesWithCounts = rdd.map(x => (x,1)).groupByKey().map(t=> (t._1,t._2.sum)).map(x=>(x._2,x._1)).sortByKey()
		val results = moviesWithCounts.collect
		for(result <- results.sorted){
			val movie = nameDict.value(result._2)
			val count = result._1
			println(s"$movie watched: $count times")
		}
	}

	def parseLines(line:String)= {
		val fields = line.split("\t")
		val movIds = fields(1)
		(movIds)
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


