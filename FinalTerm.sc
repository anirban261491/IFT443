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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD

 
object FinalTerm {
  val fileMat = Array("[1.0,  6.0]",
                    "[2.0,  8.0]",
                    "[3.0,  10.0]",
                    "[3.0,  10.0]",
                    "[4.0,  12.0]",
                    "[5.0,  14.0]"
                   )                              //> fileMat  : Array[String] = Array([1.0,  6.0], [2.0,  8.0], [3.0,  10.0], [3.
                                                  //| 0,  10.0], [4.0,  12.0], [5.0,  14.0])
 val cleanData=fileMat.map{a=>{a.replaceAll(" ","")}}
                   .map{a=>{a.replaceAll("\\[","")}}
                   .map{a=>{a.replaceAll("\\]","")}}
                                                  //> cleanData  : Array[String] = Array(1.0,6.0, 2.0,8.0, 3.0,10.0, 3.0,10.0, 4.
                                                  //| 0,12.0, 5.0,14.0)
  val XVector=cleanData.map{a=>{a.split(",")(0).toDouble}}.toList.toVector
                                                  //> XVector  : Vector[Double] = Vector(1.0, 2.0, 3.0, 3.0, 4.0, 5.0)
  val YVector=cleanData.map{a=>{a.split(",")(1).toDouble}}.toList.toVector
                                                  //> YVector  : Vector[Double] = Vector(6.0, 8.0, 10.0, 10.0, 12.0, 14.0)
 
 	def dot(v1:Vector[Double],v2:Vector[Double])={
  
  (v1 zip v2).map{ Function.tupled(_ * _)}.sum
  
  }                                               //> dot: (v1: Vector[Double], v2: Vector[Double])Double
 
 	def mean(v:Vector[Double])=v.sum/v.size   //> mean: (v: Vector[Double])Double
  
  def center(v:Vector[Double]) = {
  val m = mean(v)
  v.map( x => x - m)
  }                                               //> center: (v: Vector[Double])scala.collection.immutable.Vector[Double]
  
  def  regressYX (yVec: Vector[Double], xVec: Vector[Double] ) =  {
  val y = center(yVec)
  val x = center(xVec)
  val slopeb1 = dot(x,y) / dot(x,x)
  val intercept = mean(yVec) - slopeb1 * mean(xVec)
  (intercept, slopeb1)
}                                                 //> regressYX: (yVec: Vector[Double], xVec: Vector[Double])(Double, Double)
  
 val (intercept, slope) = regressYX(YVector, XVector)
                                                  //> intercept  : Double = 4.0
                                                  //| slope  : Double = 2.0
 
 Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT443SparkModules")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.proper
                                                  //| ties
                                                  //| SLF4J: Class path contains multiple SLF4J bindings.
                                                  //| SLF4J: Found binding in [jar:file:/Users/anirban/.ivy2/cache/org.slf4j/slf4
                                                  //| j-log4j12/jars/slf4j-log4j12-1.7.16.jar!/org/slf4j/impl/StaticLoggerBinder.
                                                  //| class]
                                                  //| SLF4J: Found binding in [jar:file:/Users/anirban/Downloads/slf4j-1.7.16/slf
                                                  //| 4j-simple-1.7.16.jar!/org/slf4j/impl/StaticLoggerBinder.class]
                                                  //| SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explana
                                                  //| tion.
                                                  //| SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSess
                                                  //| ion@13c612bd
  
  println(spark.version)                          //> 2.1.0
  
 def stripBrackets(s:String)={
 	s.replaceAll("\\[","").replaceAll("\\]","")
 }                                                //> stripBrackets: (s: String)String
  val a = fileMat.map(s => s.split(","))
         . map {a =>
           (stripBrackets(a(0).trim).toDouble,stripBrackets( a(1).trim).toDouble)}
                                                  //> a  : Array[(Double, Double)] = Array((1.0,6.0), (2.0,8.0), (3.0,10.0), (3.0
                                                  //| ,10.0), (4.0,12.0), (5.0,14.0))
  val aRDD = spark.sparkContext.makeRDD(a)        //> aRDD  : org.apache.spark.rdd.RDD[(Double, Double)] = ParallelCollectionRDD[
                                                  //| 0] at makeRDD at FinalTerm.scala:70
 aRDD.collect().foreach(println)                  //> [Stage 0:>                                                          (0 + 0
                                                  //| ) / 4]                                                                    
                                                  //|             (1.0,6.0)
                                                  //| (2.0,8.0)
                                                  //| (3.0,10.0)
                                                  //| (3.0,10.0)
                                                  //| (4.0,12.0)
                                                  //| (5.0,14.0)
def norm(v:Vector[Double])=   math.sqrt( (v.map(x => x * x)).sum )
                                                  //> norm: (v: Vector[Double])Double
val unitizeXfeature= norm(XVector)                //> unitizeXfeature  : Double = 8.0

	val parsedData  =  aRDD.map{line =>{
             LabeledPoint(line._2,
             Vectors.dense((line._1)/unitizeXfeature)) // Vectors.dense(data(0).toDouble))
             }}                                   //> parsedData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.La
                                                  //| beledPoint] = MapPartitionsRDD[1] at map at FinalTerm.scala:75
 parsedData.collect.foreach(println)              //> (6.0,[0.125])
                                                  //| (8.0,[0.25])
                                                  //| (10.0,[0.375])
                                                  //| (10.0,[0.375])
                                                  //| (12.0,[0.5])
                                                  //| (14.0,[0.625])
 var regression = new LinearRegressionWithSGD().setIntercept(true)
                                                  //> regression  : org.apache.spark.mllib.regression.LinearRegressionWithSGD = o
                                                  //| rg.apache.spark.mllib.regression.LinearRegressionWithSGD@51ec2df1
 
 regression.optimizer.setStepSize(3)              //> res0: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spar
                                                  //| k.mllib.optimization.GradientDescent@f8f56b9
 
 regression.optimizer.setNumIterations(10000)     //> res1: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spar
                                                  //| k.mllib.optimization.GradientDescent@f8f56b9
 
 val model = regression.run(parsedData)           //> 17/05/04 01:32:07 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeSystemBLAS
                                                  //| 17/05/04 01:32:07 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeRefBLAS
                                                  //| model  : org.apache.spark.mllib.regression.LinearRegressionModel = org.apac
                                                  //| he.spark.mllib.regression.LinearRegressionModel: intercept = 4.949098237491
                                                  //| 169, numFeatures = 1
 
 model.weights                                    //> res2: org.apache.spark.mllib.linalg.Vector = [13.526689639349092]
 
 model.intercept                                  //> res3: Double = 4.949098237491169

val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}                                                 //> valuesAndPreds  : org.apache.spark.rdd.RDD[(Double, Double)] = MapPartition
                                                  //| sRDD[328] at map at FinalTerm.scala:92
valuesAndPreds.collect()                          //> res4: Array[(Double, Double)] = Array((6.0,6.639934442409806), (8.0,8.33077
                                                  //| 0647328443), (10.0,10.021606852247078), (10.0,10.021606852247078), (12.0,11
                                                  //| .712443057165714), (14.0,13.403279262084352))

val linesRDD = spark.sparkContext.textFile("/Users/anirban/Documents/ScalaWorkSpace/SparkML/bodies.csv")
                                                  //> linesRDD  : org.apache.spark.rdd.RDD[String] = /Users/anirban/Documents/Sca
                                                  //| laWorkSpace/SparkML/bodies.csv MapPartitionsRDD[330] at textFile at FinalTe
                                                  //| rm.scala:98

val rowsRDD = linesRDD.map{row => row.split(",")}
                      .map{cols => Row(cols(0).trim.toInt, cols(1).trim.toDouble, cols(2).trim.toDouble, cols(3).trim.toDouble
                      ,cols(4).trim,cols(5).trim)}//> rowsRDD  : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitio
                                                  //| nsRDD[332] at map at FinalTerm.scala:101
                      
                      
val schema = StructType(List(
                 StructField("id", IntegerType, false),
                 StructField("weight", DoubleType, false),
                 StructField("height", DoubleType, false),
                 StructField("depth", DoubleType, false),
                 StructField("material", StringType, false),
                 StructField("color", StringType, false)
               )
             )                                    //> schema  : org.apache.spark.sql.types.StructType = StructType(StructField(id
                                                  //| ,IntegerType,false), StructField(weight,DoubleType,false), StructField(heig
                                                  //| ht,DoubleType,false), StructField(depth,DoubleType,false), StructField(mate
                                                  //| rial,StringType,false), StructField(color,StringType,false))
             
   import spark.implicits._
  	val BodiesDF = spark.sqlContext.createDataFrame(rowsRDD,schema)
                                                  //> BodiesDF  : org.apache.spark.sql.DataFrame = [id: int, weight: double ... 4
                                                  //|  more fields]
	 BodiesDF.show()                          //> +---+------+------+-----+--------+------+
                                                  //| | id|weight|height|depth|material| color|
                                                  //| +---+------+------+-----+--------+------+
                                                  //| |  1|  10.0|  10.0| 10.0|    wood| brown|
                                                  //| |  2|  20.0|  20.0| 20.0|   glass| green|
                                                  //| |  3|  30.0|  30.0| 30.0|   metal|yellow|
                                                  //| |  4|  33.0|  30.0| 30.0|   metal| black|
                                                  //| |  5|  40.0|  30.0| 30.0|   metal| black|
                                                  //| |  6|  40.0|  30.0| 30.0|   metal|purple|
                                                  //| |  7|  45.0|  30.0| 30.0|   metal|orange|
                                                  //| +---+------+------+-----+--------+------+
                                                  //| 
	 
	 
	 
}