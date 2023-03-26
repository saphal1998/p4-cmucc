import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

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
    
    val graphRDD = spark.sparkContext.textFile(inputGraphPath).map((row) => {
        val splits = row.split("\t")
        (splits(0).toLong, splits(1).toLong)
    })

    val users = graphRDD.flatMap{case (a,b) => Array(a,b) }.distinct

    val followers = graphRDD.aggregateByKey(Array[Long]())(seqOp, combOp).cache()
    
    // Users who have atleast one follower
    val followed_users = followers.flatMap {case(_, v) => v}.distinct

    val sourceUsers = users.subtract(followed_users)

    val numVertices = numberOfVertices

    // Prepare data with initial ranks, var used for mutablility
    var ranks = followers.mapValues(v => 1.0 / numVertices)
    var iterations = PageRankIterations

    for (i <- 1 to iterations) {
      var contribs = followers.join(ranks, followers.partitioner.get).values.flatMap { case (followees, rank) =>
        val size = followees.size
        followees.map(followee => (followee, rank / size))
      }

      val remainingWeight = (1 - contribs.values.reduce(_ + _)) / numVertices
      contribs = contribs.reduceByKey(_ + _).mapValues(x => 0.15 / numVertices + 0.85 * (x + remainingWeight))

      // They lost all of their original contribs
      val regularContribs = sourceUsers.map(x => (x, 0.15 / numVertices + 0.85 * (0 + remainingWeight)))

      // Flatten for rank
      ranks = contribs.union(regularContribs)
    }

    ranks.map(row => row._1.toString + "\t" + row._2.toString).saveAsTextFile(outputPath)
  }

  def seqOp = (accumulator: Array[Long], element: Long) => accumulator:+element

  def combOp = (accumulator1: Array[Long], accumulator2: Array[Long]) => accumulator1 ++ accumulator2
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
