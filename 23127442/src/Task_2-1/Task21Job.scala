package lab3.task21

import lab3.spark.{PromotionFrames, SparkSaleReader}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task21Job {
  def build(spark: SparkSession, input: String): DataFrame = buildFromBase(SparkSaleReader.read(spark, input))

  def validPromotionCounts(base: DataFrame): DataFrame =
    PromotionFrames.temporallyValidTokens(base, minimumSpanDays = 2)
      .groupBy("record_id")
      .agg(countDistinct(col("promotion_id")).as("valid_promotion_count"))

  def stateAverages(base: DataFrame): DataFrame =
    base.filter(
      col("state").isNotNull &&
        col("amount").isNotNull &&
        col("fulfilment") === lit("MERCHANT") &&
        col("courier_status") === lit("SHIPPED")
    ).groupBy("state")
      .agg(avg(col("amount")).as("state_average_amount"))

  def buildFromBase(base: DataFrame): DataFrame = {
    val denominator = base.filter(
      col("status").contains("CANCELLED") &&
        col("service_level") === lit("STANDARD") &&
        col("state").isNotNull &&
        col("city").isNotNull
    )

    val enriched = denominator
      .join(validPromotionCounts(base), Seq("record_id"), "left")
      .withColumn("valid_promotion_count", coalesce(col("valid_promotion_count"), lit(0L)))
      .join(stateAverages(base), Seq("state"), "left")
      .withColumn(
        "is_qualifying",
        col("valid_promotion_count") >= 3L &&
          col("amount").isNotNull &&
          col("state_average_amount").isNotNull &&
          col("amount") < col("state_average_amount")
      )

    enriched.groupBy("state", "city")
      .agg(
        count(lit(1)).as("cancelled_standard_orders"),
        sum(when(col("is_qualifying"), 1L).otherwise(0L)).cast("long").as("qualifying_orders")
      )
      .withColumn(
        "percentage",
        lit(100.0) * col("qualifying_orders").cast("double") / col("cancelled_standard_orders").cast("double")
      )
      .select("state", "city", "cancelled_standard_orders", "qualifying_orders", "percentage")
      .orderBy("state", "city")
  }
}
