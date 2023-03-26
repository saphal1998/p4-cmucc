import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{collect_list, when, size, explode_outer, sum}

object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank
    * @param numberOfVertices Number of vertices, this was determined in the previous exercise.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      outputPath: String,
      iterations: Int,
      numberOfVertices: Int,
      spark: SparkSession): Unit = {
    val graphRDD = spark.sparkContext.textFile(inputGraphPath).flatMap((value) => value.split("\n")).map((row) => {
        val splits = row.split("\t")
        (splits(0).toLong, splits(1).toLong)
    }).cache()

    val users = graphRDD.flatMap{case (a,b) => List(a,b) }

    val followers = graphRDD.aggregateByKey(List[Long]())(seqOp, combOp).cache()
    val followees = graphRDD.map{case(k,v) => (v,k)}.aggregateByKey(List[Long]())(seqOp, combOp).cache()

    val sourceUsers = users.subtract(followees.keys)
    val numVertices = numberOfVertices

    // Prepare data with initial ranks, var used for mutablility
    var ranks = followers.mapValues(v => 1.0 / numVertices)
    var iterations = 10

    for (i <- 1 to iterations) {
      var contribs = followers.join(ranks).values.flatMap { case (followees, rank) =>
        val size = followees.size
        followees.map(followee => (followee, rank / size))
      }

      val danglingContribs = (1 - contribs.values.reduce(_ + _)) / numVertices
      contribs = contribs.reduceByKey(_ + _).mapValues(x => 0.15 / numVertices + 0.85 * (x + danglingContribs))

      // They lost all of their original contribs
      val regularContribs = sourceUsers.map(x => (x, 0.15 / numVertices + 0.85 * (0 + danglingContribs)))

      // Flatten for rank
      ranks = contribs.union(regularContribs)
    }

    ranks.map(row => row._1.toString + "\t" + row._2.toString).saveAsTextFile(outputPath)
  }

  def seqOp = (accumulator: List[Long], element: Long) => accumulator:+element

  def combOp = (accumulator1: List[Long], accumulator2: List[Long]) => accumulator1 ::: accumulator2
  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val pageRankOutputPath = args(1)

    calculatePageRank(inputGraph, pageRankOutputPath, PageRankIterations, 101006458, spark)
  }
}
