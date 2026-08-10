package lab3.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object PromotionFrames {
  def tokens(base: DataFrame): DataFrame =
    base.select(
      col("record_id"),
      col("order_date"),
      explode(split(coalesce(col("promotion_ids_raw"), lit("")), ",")).as("raw_promotion_id")
    ).withColumn("promotion_id", trim(col("raw_promotion_id")))
      .filter(length(col("promotion_id")) > 0)
      .select("record_id", "order_date", "promotion_id")
      .dropDuplicates("record_id", "promotion_id")

  def lifespans(tokensFrame: DataFrame): DataFrame =
    tokensFrame.filter(col("order_date").isNotNull)
      .groupBy("promotion_id")
      .agg(
        min(col("order_date")).as("first_date"),
        max(col("order_date")).as("last_date"),
        countDistinct(col("record_id")).as("promoted_record_count")
      )
      .withColumn("span_days", datediff(col("last_date"), col("first_date")))

  def temporallyValidTokens(base: DataFrame, minimumSpanDays: Int = 2): DataFrame = {
    val tokenFrame = tokens(base)
    val validPromotions = lifespans(tokenFrame)
      .filter(col("span_days") >= lit(minimumSpanDays))
      .select("promotion_id")
    tokenFrame.join(validPromotions, Seq("promotion_id"), "inner")
  }

  def perRecordCounts(base: DataFrame, tokenFrame: DataFrame): DataFrame = {
    val counts = tokenFrame.groupBy("record_id").agg(count(lit(1)).as("promotion_count"))
    base.select("record_id").dropDuplicates("record_id")
      .join(counts, Seq("record_id"), "left")
      .withColumn("promotion_count", coalesce(col("promotion_count"), lit(0L)))
  }
}
