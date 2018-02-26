package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import scala.math.{min,max}
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostPopularMovie{
	def main(args:Array[String]){
		val sc = new SparkContext("local[*]","MinTemp")
		val names = sc.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/Marvel-names.txt")
		val n = for{
	     		l <- names.map(_.split("\""))
	     		if(l.length > 1)
     		} yield (l(0).trim.toInt,l(1))

		val lines = sc.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/Marvel-graph.txt")
		val popularity = lines.map(_.split("\\s+")).map(x =>(x(0),x.length -1 ) )
		val mostpop = popularity.groupByKey().map(t=> (t._1,t._2.sum)).sortBy(_._2, false).first
		val mospopName = n.lookup(mostpop._1.toInt)(0)
		val appeareance = mostpop._2
		println(s"mostpopular superhero is $mospopName appeared $appeareance times!")
	}

}


