import org.apache.spark.sql.SparkSession

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
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      outputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val schema = new StructType().add("follower", StringType).add("followee", StringType)
    val graphDF = spark.read.schema(schema).option("sep", "\t").csv(inputPath)
    val users = get_users(graphDF)
    val user_count = sc.broadcast(1006458)

    val rank = initialize_ranks(users)

    val followers = get_followers_per_user(graphDF)
    val following = get_following_per_user(graphDF)

    var enhanced_rank = rank.join(followers, col("followers.followee") === col("rank.user_id"), "left").select(col("user_id").as("user_id"), col("rank_value"), col("followers")).as("rank_followers").join(following, col("following.follower") === col("rank_followers.user_id"), "left").select(col("user_id"), col("rank_value"), col("followers"), col("following")).as("rank_followers_following").as("rank").cache()

    val contributions = get_contributions(enhanced_rank)

    val summed_contributions = explode_and_sum_contributions(contributions)
    val total_dangling_bonus = summed_contributions.select("contributions").where(col("contribute_to").isNull).first.getDouble(0)
    val new_ranks = calculate_rank(contributions, total_dangling_bonus)

    var number_of_iterations = 0
    while(number_of_iterations < iterations) {
        val contributions = get_contributions(enhanced_rank)
        val summed_contributions = explode_and_sum_contributions(contributions)
        val total_dangling_bonus = summed_contributions.select("contributions").where(col("contribute_to").isNull).first.getDouble(0)
        
        val new_ranks = calculate_rank(contributions, total_dangling_bonus)
        enhanced_rank = new_ranks
        number_of_iterations += 1
    }
  
    enhanced_rank.write.parquet(outputPath)

  }

  def calculate_rank(contributions: DataFrame, total_dangling_bonus: Double) = {
    val new_ranks_without_dangling = contributions
        .drop("contributions").join(summed_contributions, col("contribute_to") === col("user_id"), "left")
        .drop("contribute_to", "rank_value")
        .withColumnRenamed("contributions", "rank_value")
    new_ranks_without_dangling
        .withColumn("rank_value", col("rank_value") + (total_dangling_bonus/user_count.value))
        .withColumn("rank_value", (col("rank_value") * 0.85) + (0.15 / user_count.value))
  }

  def explode_and_sum_contributions(contributions: DataFrame) = {
      val exploded_contribution = contributions.select(col("user_id"),col("rank_value"), col("followers"), explode_outer(col("following")).as("contribute_to"), col("contributions")).as("exploded_contributions")
    exploded_contribution.groupBy("contribute_to").agg(sum("contributions").alias("contributions")).as("summed_contributions")
  }

  def get_contributions(rank: DataFrame) = {
    rank.withColumn("contributions", col("rank_value") / when(col("following").isNotNull, size(col("following"))).otherwise(1))
  }


  def get_users(graphDF: DataFrame) = {
      graphDF.select(col("followee"))
          .union(graphDF.select(col("follower")))
          .withColumnRenamed("followee","user_id").distinct.as("users")
  }

  def initialize_ranks(users: DataFrame) = {
      users.select(col("user_id"), lit(1.0/user_count.value).as("rank_value")).as("rank")
  }

  def get_followers_per_user(social_graph: DataFrame) = {
      social_graph.groupBy("followee").agg(collect_list("follower").as("followers")).as("followers")
  }

  def get_following_per_user(social_graph: DataFrame) = {
      social_graph.groupBy("follower").agg(collect_list("followee").as("following")).as("following")
  }


  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val pageRankOutputPath = args(1)

    calculatePageRank(inputGraph, pageRankOutputPath, PageRankIterations, spark)
  }
}
