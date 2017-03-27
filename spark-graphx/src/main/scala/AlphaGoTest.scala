import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wanghl on 17-3-26.
  */
object AlphaGoTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ch11").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    println(new java.io.File( "." ).getCanonicalPath)
    println(s"Running Spark Version ${sc.version}")

//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val session = SparkSession.builder().master("local").appName("alpha-go").getOrCreate()
    val df = session.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter","|").load("/home/wanghl/fdps-v3/data/reTweetNetwork-small.psv")
    df.show(5)
    println(df.count())

    val graphData = df.rdd
    println("--- The Graph Data ---")
    graphData.take(2).foreach(println)

    val vert1 = graphData.map(row => (row(3).toString.toLong,User(row(4).toString,row(5).toString,row(6).toString,row(7).toString.toInt,row(8).toString.toInt)))
    println("--- Vertices-1 ---")
    vert1.count()
    vert1.take(3).foreach(println)
    println(vert1.count())

    val vert2 = graphData.map(row => (row(9).toString.toLong,User(row(10).toString,row(11).toString,row(12).toString,row(13).toString.toInt,row(14).toString.toInt)))
    println("--- Vertices-2 ---")
    vert2.count()
    vert2.take(3).foreach(println)
    println(vert2.count)

    val vertX = vert1.++(vert2)
    println("--- Vertices-combined ---")
    println(vertX.count())

    val edgX = graphData.map(row => (Edge(row(3).toString.toLong,row(9).toString.toLong,Tweet(row(0).toString,row(1).toString.toInt))))
    println("--- Edges ---")
    edgX.take(3).foreach(println)
    println(edgX.count)

    println
    val rtGraph = Graph(vertX,edgX)
    val ranks = rtGraph.pageRank(0.1).vertices
    println("--- Page Rank ---")
    ranks.take(2).foreach(println)
    println("--- Top Users ---")
    val topUsers = ranks.sortBy(_._2,false).take(3).foreach(println)
    val topUsersWNames = ranks.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)

    println("--- How Big ? ---")
    println(rtGraph.vertices.count)
    println(rtGraph.edges.count)

    println("--- How many retweets ? ---")
    val inDegree = rtGraph.inDegrees
    val outDegree = rtGraph.outDegrees

    inDegree.take(3).foreach(println)

    inDegree.sortBy(_._2,false).take(3).foreach(println)

    outDegree.take(3).foreach(println)
    outDegree.sortBy(_._2,false).take(3).foreach(println)

    // max retweets
    println("--- Max retweets ---")
    val topRT = inDegree.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)
    val topRT1 = outDegree.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)
  }
}

case class User(name:String, location:String, tz : String, fr:Int,fol:Int)
case class Tweet(id:String,count:Int)