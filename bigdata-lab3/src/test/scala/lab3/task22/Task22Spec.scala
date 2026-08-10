package lab3.task22

import java.nio.file.Files

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class Task22Spec extends FlatSpec with Matchers {
  "Task22Pipeline exact path" should "match nearest-rank boundaries for N=1,2,5,10 and duplicate counts" in {
    withSpark { spark =>
      val base = percentileBase(spark).cache()
      val thresholds = Task22Pipeline.exactThresholds(base).collect().map { row =>
        (row.getString(0), row.getString(3)) -> row.getDouble(4)
      }.toMap
      thresholds("G1" -> "P80") shouldBe 2.0
      thresholds("G1" -> "P90") shouldBe 2.0
      thresholds("G2" -> "P80") shouldBe 4.0
      thresholds("G2" -> "P90") shouldBe 4.0
      thresholds("G5" -> "P80") shouldBe 3.0
      thresholds("G5" -> "P90") shouldBe 4.0
      thresholds("G10" -> "P80") shouldBe 7.0
      thresholds("G10" -> "P90") shouldBe 8.0
      thresholds("DUP" -> "P80") shouldBe 1.0

      val result = Task22Pipeline.finalResult(base, accuracy = 10000)
      result.count() shouldBe 20L
      result.select("sku", "month", "method", "percentile_level").distinct().count() shouldBe 20L
      result.schema.fieldNames.toVector shouldBe Vector(
        "sku", "month", "method", "percentile_level", "threshold",
        "qualifying_order_count", "amount_value_count", "amount_stddev_pop"
      )
      val g1 = result.filter("sku = 'G1' AND method = 'exact' AND percentile_level = 'P90'").first()
      g1.getLong(5) shouldBe 1L
      g1.getLong(6) shouldBe 0L
      g1.getDouble(7) shouldBe 0.0
      val duplicate = result.filter("sku = 'DUP' AND method = 'exact' AND percentile_level = 'P80'").first()
      duplicate.getLong(5) shouldBe 4L

      val profile = Task22Pipeline.groupSizeProfile(base)
      profile.maximumGroupRows shouldBe 10L
      profile.groupsAbove1000 shouldBe 0L
      base.unpersist()
    }
  }

  it should "report threshold and bidirectional qualifying-set differences" in {
    withSpark { spark =>
      val base = simpleBase(spark, "CMP", Seq(0L, 1L, 2L, 3L, 4L))
      val approximate = thresholds(spark, "CMP", 2.0, 4.0, "approx")
      val exact = thresholds(spark, "CMP", 3.0, 4.0, "exact")
      val comparison = Task22Pipeline.compare(base, approximate, exact)
      val delta = comparison.thresholdDeltas.filter("percentile_level = 'P80'").first()
      delta.getAs[Double]("threshold_delta") shouldBe -1.0
      val summary = comparison.setDifferenceSummary.filter("percentile_level = 'P80'").first()
      summary.getAs[Long]("approx_only_count") shouldBe 1L
      summary.getAs[Long]("exact_only_count") shouldBe 0L
      comparison.boundedSetDifferenceExamples.count() shouldBe 1L
    }
  }

  private val BaseSchema = StructType(Seq(
    StructField("record_id", LongType, nullable = false),
    StructField("sku", StringType, nullable = false),
    StructField("month", StringType, nullable = false),
    StructField("promotion_count", LongType, nullable = false),
    StructField("amount", DoubleType, nullable = true)
  ))

  private def percentileBase(spark: SparkSession): DataFrame = {
    val groups = Seq(
      "G1" -> Seq(2L),
      "G2" -> Seq(0L, 4L),
      "G5" -> (0L to 4L),
      "G10" -> (0L to 9L),
      "DUP" -> Seq(0L, 1L, 1L, 1L, 2L)
    )
    var id = 0L
    val rows = groups.flatMap { case (sku, counts) => counts.map { count =>
      val current = id
      id += 1L
      Row(current, sku, "2022-04", count, if (sku == "G1") null else java.lang.Double.valueOf(count.toDouble * 10.0))
    }}
    spark.createDataFrame(spark.sparkContext.parallelize(rows), BaseSchema)
  }

  private def simpleBase(spark: SparkSession, sku: String, counts: Seq[Long]): DataFrame = {
    val rows = counts.zipWithIndex.map { case (count, index) =>
      Row(index.toLong, sku, "2022-04", count, java.lang.Double.valueOf(index.toDouble))
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), BaseSchema)
  }

  private def thresholds(
    spark: SparkSession,
    sku: String,
    p80: Double,
    p90: Double,
    method: String
  ): DataFrame = {
    val schema = StructType(Seq(
      StructField("sku", StringType, false), StructField("month", StringType, false),
      StructField("method", StringType, false), StructField("percentile_level", StringType, false),
      StructField("threshold", DoubleType, false)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(sku, "2022-04", method, "P80", p80), Row(sku, "2022-04", method, "P90", p90)
    )), schema)
  }

  private def withSpark(test: SparkSession => Unit): Unit = {
    val root = Files.createTempDirectory("lab3-task22-spark-")
    val spark = SparkSession.builder().master("local[2]").appName("task22-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try test(spark) finally spark.stop()
  }
}
