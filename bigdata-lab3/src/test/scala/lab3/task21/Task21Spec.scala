package lab3.task21

import java.sql.Date
import java.nio.file.Files

import lab3.io.SingleFileExporter
import lab3.spark.{PlanEvidence, SparkSaleReader, StageCollector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class Task21Spec extends FlatSpec with Matchers {
  "Task21Job" should "match the hand-calculated state-city numerator, denominator, and percentage" in {
    withSpark { spark =>
      val base = syntheticBase(spark).cache()
      val promotionCounts = Task21Job.validPromotionCounts(base)
        .filter(col("record_id") === 2L).select("valid_promotion_count").first().getLong(0)
      promotionCounts shouldBe 3L

      val result = Task21Job.buildFromBase(base)
      val rows = result.collect().map { row =>
        (row.getString(0), row.getString(1)) -> (row.getLong(2), row.getLong(3), row.getDouble(4))
      }.toMap
      rows shouldBe Map(
        ("STATE A", "CITY SAME") -> ((2L, 1L, 50.0)),
        ("STATE A", "CITY ZERO") -> ((1L, 0L, 0.0)),
        ("STATE B", "CITY SAME") -> ((1L, 0L, 0.0))
      )
      result.schema.fieldNames.toVector shouldBe Vector(
        "state", "city", "cancelled_standard_orders", "qualifying_orders", "percentage"
      )
      base.unpersist()
    }
  }

  it should "collect extended plan, join, Exchange, and stage evidence" in {
    withSpark { spark =>
      val root = Files.createTempDirectory("lab3-task21-test-")
      val result = Task21Job.buildFromBase(syntheticBase(spark))
      val group = "task21-test-group"
      val collector = new StageCollector(group)
      spark.sparkContext.addSparkListener(collector)
      spark.sparkContext.setJobGroup(group, "Task21 test output")
      try result.count() shouldBe 3L
      finally spark.sparkContext.clearJobGroup()
      val evidence = root.resolve("plan.txt")
      val plan = PlanEvidence.writeExtendedPlan(result, evidence)
      plan should include("Join")
      val physical = PlanEvidence.executedPlan(result)
      PlanEvidence.joinStrategies(physical) should not be empty
      PlanEvidence.countExchangeNodes(physical) should be > 0
      val deadline = System.currentTimeMillis() + 5000L
      while (collector.stageIds.isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(25L)
      collector.stageIds should not be empty
    }
  }

  it should "write one exact readable Parquet file on a supported Hadoop local filesystem" in {
    if (System.getProperty("os.name", "").toLowerCase.contains("windows")) {
      cancel("Spark/Hadoop 3.3.6 Parquet output requires the native Windows layer; this test runs on Linux/Lab 1")
    }
    withSpark { spark =>
      val output = Files.createTempDirectory("lab3-task21-output-").resolve("Task_2-1.parquet")
      SingleFileExporter.exportParquet(Task21Job.buildFromBase(syntheticBase(spark)), output, overwrite = false)
      Files.isRegularFile(output) shouldBe true
      spark.read.parquet(output.toString).count() shouldBe 3L
    }
  }

  private def syntheticBase(spark: SparkSession): DataFrame = {
    val fixture = getClass.getResource("/fixtures/shared-sales.csv").toURI.toString
    val schema = SparkSaleReader.read(spark, fixture).schema
    val rows = Seq(
      row(0, "04-01", "SHIPPED", "MERCHANT", "STANDARD", "SHIPPED", 100.0, "SOURCE", "STATE A", "P1,P2,Amazon Deal,SPAN1"),
      row(1, "04-03", "SHIPPED", "MERCHANT", "STANDARD", "SHIPPED", 200.0, "SOURCE", "STATE A", "P1,P2,Amazon Deal"),
      row(2, "04-02", "CANCELLED", "MERCHANT", "STANDARD", null, 100.0, "CITY SAME", "STATE A", "P1,P2,Amazon Deal,P1"),
      row(3, "04-02", "CANCELLED", "MERCHANT", "STANDARD", null, null, "CITY SAME", "STATE A", ""),
      row(4, "04-02", "CANCELLED", "MERCHANT", "STANDARD", null, 90.0, "CITY ZERO", "STATE A", "SPAN1"),
      row(5, "04-02", "CANCELLED", "AMAZON", "STANDARD", null, 10.0, "CITY SAME", "STATE B", "P1,P2,Amazon Deal")
    )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private def row(
    id: Long,
    day: String,
    status: String,
    fulfilment: String,
    service: String,
    courier: String,
    amount: java.lang.Double,
    city: String,
    state: String,
    promotions: String
  ): Row = Row(
    id,
    "ORDER-" + id,
    Date.valueOf("2022-" + day),
    status,
    fulfilment,
    service,
    "STYLE",
    "SKU",
    "XXL",
    courier,
    1L,
    amount,
    city,
    state,
    promotions,
    "2022-04",
    status.contains("SHIPPED")
  )

  private def withSpark(test: SparkSession => Unit): Unit = {
    val root = Files.createTempDirectory("lab3-task21-spark-")
    val spark = SparkSession.builder().master("local[2]").appName("task21-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try test(spark) finally spark.stop()
  }
}
