package kg.baaber.spark

import org.apache.spark.{SparkContext, SparkConf}

object RatingsCounter{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","RatingsCounter")
		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/ml-100k/u.data")
		val ratings = lines.map(x => x.toString().split("\t")(2))
		val results = ratings.countByValue()
		val sortedResults  =results.toSeq.sortBy(_._1)
		sortedResults.foreach(println)
	}

}