package scala_package
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext


object Main extends App {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext("local[10]" , "SparkDemo")

    val spark = SparkSession
      .builder()
      .appName("FirstApplication")
      .getOrCreate()

    class DataAccess (
                       var Name: String
                     ) {

        def get_spark_csv(file_path: String): sql.DataFrame ={
            var train_spark = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(file_path)
            return train_spark
        }
        def get_parallel_from_spark_df(train_selected: sql.DataFrame): sql.DataFrame ={
            val spark = SparkSession.builder.getOrCreate()
            import spark.implicits._
            var b = sc.parallelize(train_selected.take(5000))
            var train_selected_parallel = train_selected.sqlContext.createDataFrame(b, train_selected.schema)
            return train_selected_parallel
        }

    }
    val data_access = new DataAccess("csv")
    val train_selected = data_access.get_spark_csv("C:\\Users\\ceaus\\Desktop\\sample_project_1\\train.csv")
    var train_selected_parallel = data_access.get_parallel_from_spark_df(train_selected)
    val t1 = System.nanoTime
    val train_selected_variance = train_selected_parallel.groupBy("Region").agg(variance("Total_Revenue") as "Rev_Variance")
    val duration = (System.nanoTime - t1) / 1e9d
    println("time to groupby Region and get variance (in seconds): ",duration)
    train_selected_variance.take(10).foreach(println)
}