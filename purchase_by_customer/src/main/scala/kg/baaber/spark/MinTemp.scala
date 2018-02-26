package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import scala.math.{min,max}

object MinTemp{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","MinTemp")
		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/customer-orders.csv")
		val rdd = lines.map(parseLines)
		val amounts = rdd.groupByKey().map(t=> (t._1,t._2.sum)).map(x=>(x._2,x._1)).sortByKey()
		val results = amounts.collect
		for(result <- results.sorted){
			val customer = result._2
			val amount = result._1
			println(s"$customer spent in total: $amount")
		}
	}

	def parseLines(line:String)= {
		val fields = line.split(",")
		val custIds = fields(0)
		val items = fields(1)
		val amountsSpend = fields(2).toFloat
		(custIds,amountsSpend)
	}


}
