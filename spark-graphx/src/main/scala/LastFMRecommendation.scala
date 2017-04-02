import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.split
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable.HashMap;

/**
  * Created by wanghl on 17-4-1.
  */
object LastFMRecommendation  {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LastFMDemo").setMaster("spark://big-data1:7077").setJars(List("/home/wanghl/IdeaProjects/learning-graph/spark-graphx/target/spark-graphx-1.0.jar"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    println(new java.io.File(".").getCanonicalPath)
    println(s"Running Spark Version ${sc.version}")

    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession.builder().master("spark://big-data1:7077").appName("LastFMDemo").getOrCreate()
    val rawArtistAlias = sc.textFile("hdfs://big-data1:9000/user/ds/artist_alias.txt");
    println(rawArtistAlias.count())

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      }
      else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()


    val broadcast = sc.broadcast(artistAlias);

    val ratings = spark.read.option("header", false).option("inferSchema", true).text("hdfs://big-data1:9000/user/ds/user_artist_data.txt");
    ratings.show(5, false);
    ratings.printSchema();
    System.out.println(ratings.count());

    val ratings1 = ratings.select(split(ratings("value"), "\\s")).as("values");
    ratings1.show(5, false);

    val ratings2 = ratings1.rdd.map{x =>
      val list = x.getList(0)
      Rating(Integer.parseInt(list.get(0)), broadcast.value.getOrElse(Integer.parseInt(list.get(1)), Integer.parseInt(list.get(1))), java.lang.Double.parseDouble(list.get(2)))
    };


    val ratings3 = spark.createDataFrame(ratings2);
    ratings3.show(5)
    val Array(train, test) = ratings3.randomSplit(Array(0.8, 0.2))
    println("Train = "+train.count()+" Test = "+test.count())
    val algALS = new ALS();
    algALS.setItemCol("product");
    algALS.setRank(12);
    algALS.setRegParam(0.1); // was regularization parameter, was lambda in MLlib
    algALS.setMaxIter(20);
    val mdlReco = algALS.fit(train);

    val predictions = mdlReco.transform(test);
    predictions.show(5);
    predictions.printSchema();

    val pred = predictions.na.drop();
    System.out.println("Orig = "+predictions.count()+" Final = "+ pred.count() + " Dropped = "+ (predictions.count() - pred.count()));
    val evaluator = new RegressionEvaluator();
    evaluator.setLabelCol("rating");
    val rmse = evaluator.evaluate(pred);
    System.out.println("Root Mean Squared Error = " + rmse);

    spark.stop();
    sc.stop();
  }
}
