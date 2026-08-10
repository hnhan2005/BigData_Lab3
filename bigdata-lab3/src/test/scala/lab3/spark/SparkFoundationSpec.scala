package lab3.spark

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class SparkFoundationSpec extends FlatSpec with Matchers {
  "Spark foundation" should "read, normalize, tokenize, profile, and capture execution evidence on local[2]" in {
    val temporary = Files.createTempDirectory("lab3-spark-foundation-")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("lab3-spark-foundation-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.warehouse.dir", temporary.resolve("warehouse").toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val fixture = Paths.get(getClass.getResource("/fixtures/shared-sales.csv").toURI).toString
      val base = SparkSaleReader.read(spark, fixture).cache()
      base.count() shouldBe 3L
      base.filter(col("record_id") === 0L).select("city").first().getString(0) shouldBe "CITY, A"

      val tokens = PromotionFrames.tokens(base)
      tokens.count() shouldBe 3L
      val counts = PromotionFrames.perRecordCounts(base, tokens)
        .orderBy("record_id").collect().map(row => row.getLong(0) -> row.getLong(1)).toMap
      counts shouldBe Map(0L -> 2L, 1L -> 0L, 2L -> 1L)
      PromotionFrames.temporallyValidTokens(base).count() shouldBe 0L

      val quality = DataQuality.collect(base)
      quality.totalRows shouldBe 3L
      quality.nullAmounts shouldBe 2L
      quality.duplicateOrderIds shouldBe 0L
      quality.minimumDate shouldBe Some("2022-04-01")
      quality.maximumDate shouldBe Some("2022-04-03")
      quality.maximumSkuMonthRows shouldBe 2L

      val joined = base.select("record_id").join(countsDataFrame(base), Seq("record_id"), "left")
      val group = "lab3-foundation-stage-test"
      val collector = new StageCollector(group)
      spark.sparkContext.addSparkListener(collector)
      spark.sparkContext.setJobGroup(group, "foundation integration")
      joined.count() shouldBe 3L
      spark.sparkContext.clearJobGroup()

      val evidence = temporary.resolve("extended-plan.txt")
      val extended = PlanEvidence.writeExtendedPlan(joined, evidence)
      extended should include("Join")
      Files.size(evidence) should be > 0L
      val physical = PlanEvidence.executedPlan(joined)
      PlanEvidence.countExchangeNodes(physical) should be >= 0
      PlanEvidence.joinStrategies(physical) should not be empty

      val deadline = System.currentTimeMillis() + 5000L
      while (collector.stageIds.isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(25L)
      collector.stageIds should not be empty
      base.unpersist()
    } finally spark.stop()
  }

  private def countsDataFrame(base: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame =
    PromotionFrames.perRecordCounts(base, PromotionFrames.tokens(base))
}
