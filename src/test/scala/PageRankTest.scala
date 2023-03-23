import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class PageRankTest
  extends FunSpec
  with TestingUtil
  with MustMatchers
  with LocalSparkSession
  with BeforeAndAfterEach {

  val Iterations = 10
  val InputGraphPath = "data/SmallGraph1"
  val OutputGraphPath = "data/SmallGraph1-Output"
  val ReferencePath = "data/SmallGraph1-Reference"

  /**
   * This function gets call before the execution of each test.
   * If adding tests of your own, make sure to delete your files as well.
   */
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    FileUtils.deleteQuietly(new File(OutputGraphPath))
  }

  describe("the PageRank algorithm") {
    it("should match the expected output for SmallGraph1") {
      PageRank.calculatePageRank(InputGraphPath, OutputGraphPath, Iterations, spark)
      comparePageRanks(OutputGraphPath, ReferencePath)
    }
  }

}
