package scala_package
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, functions}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.ListBuffer


object Main extends App {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext("local[10]", "SparkDemo")
    val spark = SparkSession
      .builder()
      .appName("FirstApplication")
      .getOrCreate()
    class DataAccess(
                      var Name: String
                    ) {

        def get_spark_csv(file_path: String): sql.DataFrame = {
            var train_spark = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(file_path)
            return train_spark
        }

        def get_parallel_from_spark_df(train_selected: sql.DataFrame): sql.DataFrame = {
            val spark = SparkSession.builder.getOrCreate()
            import spark.implicits._
            var b = sc.parallelize(train_selected.take(5000))
            var train_selected_parallel = train_selected.sqlContext.createDataFrame(b, train_selected.schema)
            return train_selected_parallel
        }
    }
    val data_access = new DataAccess("csv")
    val train_spark = data_access.get_spark_csv("C:\\Users\\ceaus\\Desktop\\sample_project_1\\train.csv")
    val df = train_spark.toDF()
    import spark.implicits._
    import Summarizer._
    val df4 = df.filter($"Region"==="Asia")
    var list= new ListBuffer[(Vector,Int)]()
    for(element <- df.filter($"Region"==="Asia").select("Total_Revenue").collect.map(_.getDouble(0)))
    {
        list += ((Vectors.dense(element),1))
    }
    val seq1 = list.toSeq
    val df2= seq1.toDF("Total_Revenue","weight")
    //
    var t0 = System.nanoTime()
    val train_selected_variance = df4.agg(functions.variance("Total_Revenue") as "Rev_Variance")
    var t1 = System.nanoTime()
    val t1Time =  (t1 - t0).toDouble
    t0 = System.nanoTime()
    val varianceRev1 = df2.select(variance($"Total_Revenue")).first()
    t1 = System.nanoTime()
    val t2Time =  (t1 - t0).toDouble
    t0 = System.nanoTime()
    val meanVal3 = (df2.select(std($"Total_Revenue")).first())
    val a = meanVal3.getAs[Vector](0)
    val gg = a.toArray
    val varianceRev2 = gg(0) * gg(0)
    t1 = System.nanoTime()
    val t3Time =  (t1 - t0).toDouble
    //
    println("time(s) in seconds to find variance using agg: ",t1Time/1000000000)
    println("time(s) in seconds to find variance using select variance: ",t2Time/1000000000)
    println("time(s) in seconds to find variance using select std^2: ",t3Time/1000000000)
    println("variance using agg")
    train_selected_variance.take(10).foreach(println)
    println("variance using select variance",varianceRev1)
    println("variance using select stfd^2",varianceRev2)


}