
package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import scala.math.{min,max}

object PurchaseByCustomer{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","MinTemp")
		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/1800.csv")
		val rdd = lines.map(parseLines)
		val onlyMin = rdd.filter(x => x._2 =="TMAX")
		val stationTemps = onlyMin.map(x => (x._1,x._3))
		val minTempStations = stationTemps.reduceByKey((x,y) => max(x,y))
		val results = minTempStations.collect
		for(result <- results.sorted){
			val station = result._1
			val temp = result._2
			val formattedTemp = f"$temp%.2f F"
			println(s"$station minimum temperetaure: $formattedTemp")
		}
	}

	def parseLines(line:String)= {
		val fields = line.split(",")
		val stationIds = fields(0)
		val tmpStatus = fields(2)
		val temp = fields(3).toFloat * 0.1f*(9f/5f)+32f
		(stationIds,tmpStatus,temp)
	}


}