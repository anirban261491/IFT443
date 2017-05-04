import org.apache.log4j.{Level, Logger}
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.rdd.RDD

case class Body(id : BigInt, width: Double, height: Double, depth: Double, material: String, color: String)
object Finaterm2 {
   Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT443SparkModules")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| SLF4J: Class path contains multiple SLF4J bindings.
                                                  //| SLF4J: Found binding in [jar:file:/Users/anirban/.ivy2/cache/org.slf4j/slf4j
                                                  //| -log4j12/jars/slf4j-log4j12-1.7.16.jar!/org/slf4j/impl/StaticLoggerBinder.cl
                                                  //| ass]
                                                  //| SLF4J: Found binding in [jar:file:/Users/anirban/Downloads/slf4j-1.7.16/slf4
                                                  //| j-simple-1.7.16.jar!/org/slf4j/impl/StaticLoggerBinder.class]
                                                  //| SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanat
                                                  //| ion.
                                                  //| SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@42deb43a
  import spark.implicits._
  println(spark.version)                          //> 2.1.0
  
  
  val linesRDD = spark.sparkContext.textFile("/Users/anirban/Documents/ScalaWorkSpace/SparkML/bodies.csv")
                                                  //> linesRDD  : org.apache.spark.rdd.RDD[String] = /Users/anirban/Documents/Scal
                                                  //| aWorkSpace/SparkML/bodies.csv MapPartitionsRDD[1] at textFile at Finaterm2.s
                                                  //| cala:24
  
  val rowsRDD = linesRDD.map{row => row.split(",")}
                      .map{cols => Row(cols(0).trim.toInt, cols(1).trim.toDouble, cols(2).trim.toDouble, cols(3).trim.toDouble
                      ,cols(4).trim,cols(5).trim)}//> rowsRDD  : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitio
                                                  //| nsRDD[3] at map at Finaterm2.scala:27
                      
                      
	val schema = StructType(List(
                 StructField("id", IntegerType, false),
                 StructField("width", DoubleType, false),
                 StructField("height", DoubleType, false),
                 StructField("depth", DoubleType, false),
                 StructField("material", StringType, false),
                 StructField("color", StringType, false)
               )
             )                                    //> schema  : org.apache.spark.sql.types.StructType = StructType(StructField(id
                                                  //| ,IntegerType,false), StructField(width,DoubleType,false), StructField(heigh
                                                  //| t,DoubleType,false), StructField(depth,DoubleType,false), StructField(mater
                                                  //| ial,StringType,false), StructField(color,StringType,false))
  val BodiesDF = spark.sqlContext.createDataFrame(rowsRDD,schema)
                                                  //> BodiesDF  : org.apache.spark.sql.DataFrame = [id: int, width: double ... 4 
                                                  //| more fields]
	 BodiesDF.show()                          //> +---+-----+------+-----+--------+------+
                                                  //| | id|width|height|depth|material| color|
                                                  //| +---+-----+------+-----+--------+------+
                                                  //| |  1| 10.0|  10.0| 10.0|    wood| brown|
                                                  //| |  2| 20.0|  20.0| 20.0|   glass| green|
                                                  //| |  3| 30.0|  30.0| 30.0|   metal|yellow|
                                                  //| |  4| 33.0|  30.0| 30.0|   metal| black|
                                                  //| |  5| 40.0|  30.0| 30.0|   metal| black|
                                                  //| |  6| 40.0|  30.0| 30.0|   metal|purple|
                                                  //| |  7| 45.0|  30.0| 30.0|   metal|orange|
                                                  //| +---+-----+------+-----+--------+------+
                                                  //| 
	 
	 val BodiesDS = BodiesDF.as[Body]         //> BodiesDS  : org.apache.spark.sql.Dataset[Body] = [id: int, width: double ..
                                                  //| . 4 more fields]
   BodiesDS.show()                                //> +---+-----+------+-----+--------+------+
                                                  //| | id|width|height|depth|material| color|
                                                  //| +---+-----+------+-----+--------+------+
                                                  //| |  1| 10.0|  10.0| 10.0|    wood| brown|
                                                  //| |  2| 20.0|  20.0| 20.0|   glass| green|
                                                  //| |  3| 30.0|  30.0| 30.0|   metal|yellow|
                                                  //| |  4| 33.0|  30.0| 30.0|   metal| black|
                                                  //| |  5| 40.0|  30.0| 30.0|   metal| black|
                                                  //| |  6| 40.0|  30.0| 30.0|   metal|purple|
                                                  //| |  7| 45.0|  30.0| 30.0|   metal|orange|
                                                  //| +---+-----+------+-----+--------+------+
                                                  //| 
  
 val area = udf((w:Double, h: Double, l:Double) => 2*l*w+2*l*h+2*h*w )
                                                  //> area  : org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedF
                                                  //| unction(<function3>,DoubleType,Some(List(DoubleType, DoubleType, DoubleType
                                                  //| )))
 

 
val BodiesDS1= BodiesDS.withColumn("Area",area(BodiesDS("width"),BodiesDS("height"),BodiesDS("depth")))
                                                  //> BodiesDS1  : org.apache.spark.sql.DataFrame = [id: int, width: double ... 5
                                                  //|  more fields]
 BodiesDS1.show()                                 //> +---+-----+------+-----+--------+------+------+
                                                  //| | id|width|height|depth|material| color|  Area|
                                                  //| +---+-----+------+-----+--------+------+------+
                                                  //| |  1| 10.0|  10.0| 10.0|    wood| brown| 600.0|
                                                  //| |  2| 20.0|  20.0| 20.0|   glass| green|2400.0|
                                                  //| |  3| 30.0|  30.0| 30.0|   metal|yellow|5400.0|
                                                  //| |  4| 33.0|  30.0| 30.0|   metal| black|5760.0|
                                                  //| |  5| 40.0|  30.0| 30.0|   metal| black|6600.0|
                                                  //| |  6| 40.0|  30.0| 30.0|   metal|purple|6600.0|
                                                  //| |  7| 45.0|  30.0| 30.0|   metal|orange|7200.0|
                                                  //| +---+-----+------+-----+--------+------+------+
                                                  //| 
}