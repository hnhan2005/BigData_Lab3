package lab3.task22

import lab3.spark.PromotionFrames
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

final case class ComparisonArtifacts(
  thresholdDeltas: DataFrame,
  setDifferenceSummary: DataFrame,
  boundedSetDifferenceExamples: DataFrame
)

final case class GroupSizeProfile(
  validRows: Long,
  groupCount: Long,
  maximumGroupRows: Long,
  groupsAbove1000: Long,
  estimatedBytesPerRow: Long,
  estimatedMaximumGroupBytes: Long
)

object Task22Pipeline {
  def skuMonthBase(base: DataFrame): DataFrame = {
    val promotionCounts = PromotionFrames.tokens(base)
      .groupBy("record_id")
      .agg(count(lit(1)).cast("long").as("promotion_count"))

    base.filter(
      col("record_id").isNotNull && col("sku").isNotNull && col("month").isNotNull && col("order_date").isNotNull
    ).select("record_id", "sku", "month", "amount")
      .join(promotionCounts, Seq("record_id"), "left")
      .withColumn("promotion_count", coalesce(col("promotion_count"), lit(0L)))
      .select("record_id", "sku", "month", "promotion_count", "amount")
  }

  def approximateThresholds(skuMonth: DataFrame, accuracy: Int): DataFrame = {
    require(accuracy > 0, "accuracy must be positive")
    skuMonth.groupBy("sku", "month")
      .agg(expr("percentile_approx(promotion_count, array(0.8, 0.9), " + accuracy + ")").as("thresholds"))
      .select(col("sku"), col("month"), posexplode(col("thresholds")).as(Seq("position", "threshold")))
      .withColumn("percentile_level", when(col("position") === 0, lit("P80")).otherwise(lit("P90")))
      .withColumn("method", lit("approx"))
      .select(
        col("sku"),
        col("month"),
        col("method"),
        col("percentile_level"),
        col("threshold").cast("double").as("threshold")
      )
  }

  def exactThresholds(skuMonth: DataFrame): DataFrame = {
    val partition = Window.partitionBy("sku", "month")
    val ordered = partition.orderBy(col("promotion_count").asc, col("record_id").asc)
    val ranked = skuMonth
      .withColumn("group_size", count(lit(1)).over(partition))
      .withColumn("row_number", row_number().over(ordered))

    def threshold(level: String, percentile: Double): DataFrame =
      ranked.filter(
        col("row_number") === ceil(col("group_size").cast("double") * lit(percentile)).cast("long")
      ).select(
        col("sku"),
        col("month"),
        lit("exact").as("method"),
        lit(level).as("percentile_level"),
        col("promotion_count").cast("double").as("threshold")
      )

    threshold("P80", 0.8).unionByName(threshold("P90", 0.9))
  }

  def qualifyingStatistics(skuMonth: DataFrame, thresholds: DataFrame): DataFrame = {
    val aggregated = skuMonth.join(thresholds, Seq("sku", "month"), "inner")
      .filter(col("promotion_count").cast("double") >= col("threshold"))
      .groupBy("sku", "month", "method", "percentile_level", "threshold")
      .agg(
        count(lit(1)).as("qualifying_order_count"),
        count(col("amount")).as("amount_value_count"),
        stddev_pop(col("amount")).as("raw_amount_stddev_pop")
      )

    aggregated.withColumn(
      "amount_stddev_pop",
      when(col("qualifying_order_count") < 2L || col("amount_value_count") === 0L, lit(0.0))
        .otherwise(coalesce(col("raw_amount_stddev_pop"), lit(0.0)))
    ).select(
      "sku",
      "month",
      "method",
      "percentile_level",
      "threshold",
      "qualifying_order_count",
      "amount_value_count",
      "amount_stddev_pop"
    )
  }

  def finalResult(skuMonth: DataFrame, accuracy: Int): DataFrame = {
    val thresholds = approximateThresholds(skuMonth, accuracy).unionByName(exactThresholds(skuMonth))
    qualifyingStatistics(skuMonth, thresholds)
      .orderBy("sku", "month", "method", "percentile_level")
  }

  def compare(skuMonth: DataFrame, approximate: DataFrame, exact: DataFrame): ComparisonArtifacts = {
    val keys = Seq("sku", "month", "percentile_level")
    val approximateNamed = approximate.select(
      col("sku"), col("month"), col("percentile_level"), col("threshold").as("approx_threshold")
    )
    val exactNamed = exact.select(
      col("sku"), col("month"), col("percentile_level"), col("threshold").as("exact_threshold")
    )
    val deltas = approximateNamed.join(exactNamed, keys, "inner")
      .withColumn("threshold_delta", col("approx_threshold") - col("exact_threshold"))

    def qualifying(thresholds: DataFrame): DataFrame =
      skuMonth.join(thresholds, Seq("sku", "month"), "inner")
        .filter(col("promotion_count").cast("double") >= col("threshold"))
        .select("sku", "month", "percentile_level", "record_id")

    val approximateSet = qualifying(approximate)
    val exactSet = qualifying(exact)
    val setKeys = keys :+ "record_id"
    val onlyApprox = approximateSet.join(exactSet, setKeys, "left_anti")
      .withColumn("difference_side", lit("approx_only"))
    val onlyExact = exactSet.join(approximateSet, setKeys, "left_anti")
      .withColumn("difference_side", lit("exact_only"))
    val differences = onlyApprox.unionByName(onlyExact)

    val counts = differences.groupBy(keys.map(col): _*)
      .agg(
        sum(when(col("difference_side") === "approx_only", 1L).otherwise(0L)).as("approx_only_count"),
        sum(when(col("difference_side") === "exact_only", 1L).otherwise(0L)).as("exact_only_count")
      )
    val summary = deltas.select(keys.map(col): _*).join(counts, keys, "left")
      .na.fill(0L, Seq("approx_only_count", "exact_only_count"))

    ComparisonArtifacts(
      thresholdDeltas = deltas.orderBy(keys.map(col): _*),
      setDifferenceSummary = summary.orderBy(keys.map(col): _*),
      boundedSetDifferenceExamples = differences.orderBy((keys :+ "record_id").map(col): _*).limit(100)
    )
  }

  def groupSizeProfile(skuMonth: DataFrame, estimatedBytesPerRow: Long = 128L): GroupSizeProfile = {
    require(estimatedBytesPerRow > 0L, "estimatedBytesPerRow must be positive")
    val sizes = skuMonth.groupBy("sku", "month").count().cache()
    try {
      val summary = sizes.agg(
        count(lit(1)).as("group_count"),
        coalesce(max(col("count")), lit(0L)).as("maximum_group_rows"),
        sum(when(col("count") > 1000L, 1L).otherwise(0L)).as("groups_above_1000"),
        coalesce(sum(col("count")), lit(0L)).as("valid_rows")
      ).first()
      val maximum = summary.getAs[Long]("maximum_group_rows")
      GroupSizeProfile(
        validRows = summary.getAs[Long]("valid_rows"),
        groupCount = summary.getAs[Long]("group_count"),
        maximumGroupRows = maximum,
        groupsAbove1000 = Option(summary.getAs[java.lang.Long]("groups_above_1000")).fold(0L)(_.longValue()),
        estimatedBytesPerRow = estimatedBytesPerRow,
        estimatedMaximumGroupBytes = maximum * estimatedBytesPerRow
      )
    } finally sizes.unpersist()
  }
}
