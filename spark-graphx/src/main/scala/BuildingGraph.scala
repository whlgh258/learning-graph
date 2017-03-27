import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wanghl on 17-3-26.
  */
object BuildingGraph {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("ch11").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    println(sc.version)

    val defaultPerson = Person("NA",0)
    val vertexList = List(
      (1L, Person("Alice", 18)),
      (2L, Person("Bernie", 17)),
      (3L, Person("Cruz", 15)),
      (4L, Person("Donald", 12)),
      (5L, Person("Ed", 15)),
      (6L, Person("Fran", 10)),
      (7L, Person("Genghis",854))
    )

    val edgeList = List(
      Edge(1L, 2L, 5),
      Edge(1L, 3L, 1),
      Edge(3L, 2L, 5),
      Edge(2L, 4L, 12),
      Edge(4L, 5L, 4),
      Edge(5L, 6L, 2),
      Edge(6L, 7L, 2),
      Edge(7L, 4L, 5),
      Edge(6L, 4L, 4)
    )

    val vertexRDD = sc.parallelize(vertexList)
    val edgeRDD = sc.parallelize(edgeList)
    val graph = Graph(vertexRDD, edgeRDD,defaultPerson)

    /*println(graph.numVertices)
    println(graph.numEdges)

    val vertices = graph.vertices;
    vertices.collect.foreach(println);

    val edges = graph.edges
    edges.collect.foreach(println)

    val triplets = graph.triplets
    println(triplets.count())
    triplets.take(3)
    triplets.map(t=>t.toString).collect().foreach(println)

    val inDeg = graph.inDegrees // Followers
    inDeg.collect().foreach(println)
    val outDeg = graph.outDegrees // Follows
    outDeg.collect().foreach(println)
    val allDeg = graph.degrees
    allDeg.collect().foreach(println)

    val g1 = graph.subgraph((edge) => edge.attr > 4)
    g1.triplets.collect.foreach(println)
    println("====================")
    val g2 = graph.subgraph(vpred = (id, person) => person.age > 21)
    g2.triplets.collect.foreach(println) // empty
    println("--------------------------")
    g2.vertices.collect.foreach(println)
    println("+++++++++++++++++++++++++++++++")
    g2.edges.collect.foreach(println) // empty
    println("******************************")
    val g3 = graph.subgraph(vpred = (id, person) => person.age >= 18)
    g3.triplets.collect.foreach(println) // empty
    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    g3.vertices.collect.foreach(println)
    g3.edges.collect().foreach(println) //empty*/

    /*val cc = graph.connectedComponents()
    cc.triplets.collect.foreach(println)
    graph.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect.foreach(println)
    println(cc.vertices.map(_._2).collect.distinct.size)

    cc.vertices.groupBy(_._2).map(p=>(p._1,p._2.size)).
      sortBy(x=>x._2,false). // sortBy(keyFunc,ascending)
      collect().foreach(println)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val ccS = graph.stronglyConnectedComponents(10)
    ccS.triplets.collect.foreach(println)
    ccS.vertices.map(_.swap).groupByKey.map(_._2).collect.foreach(println)
    println(ccS.vertices.map(_._2).collect.distinct.size) // No. of connected components
    val triCounts = graph.triangleCount()
    println(triCounts)
    val triangleCounts = triCounts.vertices.collect
    triangleCounts.foreach(println)*/

    val ranks = graph.pageRank(0.1).vertices
    ranks.collect().foreach(println)
    val topVertices = ranks.sortBy(_._2,false).collect.foreach(println)

    val oldestFollower = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age),//sendMsg
      (x,y) => math.max(x,y) //mergeMsg
    )
    oldestFollower.collect().foreach(println)
    println
    val oldestFollowee = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToSrc(edgeContext.dstAttr.age),//sendMsg
      (x,y) => math.max(x,y) //mergeMsg
    )
    oldestFollowee.collect().foreach(println)
    println

    val youngestFollower = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age),//sendMsg
      (x,y) => math.min(x,y) //mergeMsg
    )
    youngestFollower.collect().foreach(println)
    println

    val youngestFollowee = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToSrc(edgeContext.dstAttr.age),//sendMsg
      (x,y) => math.min(x,y) //mergeMsg
    )
    youngestFollowee.collect().foreach(println)
    println

    var iDegree = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToDst(1),//sendMsg
      (x,y) => x+y //mergeMsg
    )
    iDegree.collect().foreach(println)
    graph.inDegrees.collect().foreach(println)
    println

    val oDegree = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToSrc(1),//sendMsg
      (x,y) => x+y //mergeMsg
    )
    oDegree.collect().foreach(println)
    graph.outDegrees.collect().foreach(println)
  }
}

case class Person(name:String,age:Int)