package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}


object FriendCounter{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","FriendCounter")
		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/fakefriends.csv")
		val rdd = lines.map(parseLines)
		val reducedKeys = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
		val avarage = reducedKeys.mapValues(x=>(x._1/x._2))
		val result = avarage.collect()
		result.sorted.foreach(println)
	}

	def parseLines(line:String)= {
		val fields = line.split(",")
		val names = fields(1)
		val friends = fields(3).toInt
		(names,friends)
	}
}