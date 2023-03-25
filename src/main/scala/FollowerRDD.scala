import org.apache.spark.SparkContext

object FollowerRDD {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path with userID and
    * count **tab separated**.
    *
    * It must be done using the RDD API.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param sc the SparkContext.
    */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    // TODO: Calculate the follower count for each user	
    // TODO: Write the top 100 users to the outputPath with userID and count **tab separated**
    val graphRDD = sc.textFile(inputPath)
    val follower_followee_rows = graphRDD.flatMap((value) => value.split("\n")).distinct
    follower_followee_rows.cache()
    val follower_followee = follower_followee_rows.map((row) => row.split("\t")).map((row) => (row(1), 1))
    val follower_count = follower_followee.reduceByKey((value1, value2) => value1 + value2).sortBy(-_._2)
    val top100RDD = sc.parallelize(follower_count.take(100)).map(row => row._1.toString + "\t" + row._2.toString)
    top100RDD.saveAsTextFile(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
