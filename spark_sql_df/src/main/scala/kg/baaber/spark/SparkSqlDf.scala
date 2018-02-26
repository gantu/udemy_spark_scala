package kg.baaber.spark


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

object SparkSqlDf{

	case class Person(id:Int, name:String, age:Int, numFriends:Int)

	def mapper(lines:String):Person = {
		val fields = lines.split(',')
		val person = Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
		person
	}

	def main(args:Array[String]){
		val spark = SparkSession
		.builder
		.appName("SparkSqlDf")
		.master("local[*]")
		.getOrCreate()


		val lines = spark.sparkContext.textFile("/home/wild/projects/udemy_spark_scala/SparkScala/fakefriends.csv")
		val people = lines.map(mapper)

		import spark.implicits._
		val schemaPeople = people.toDS.cache()

		
		println("Schema is: ")
		schemaPeople.printSchema()

		println("Only Names")
		schemaPeople.select("name").show()

		println("Only ages > 20")
		schemaPeople.filter(schemaPeople("age") > 20).show()

		println("Group by Age")
		schemaPeople.groupBy("age").count().show()

		println("age edited")
		schemaPeople.select(schemaPeople("name"),schemaPeople("age") + 10).show()

		//schemaPeople.createOrReplaceTempView("people")
		//val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

		//val results = teenagers.collect()

		//results.foreach(println)


		spark.stop()
	}

}


