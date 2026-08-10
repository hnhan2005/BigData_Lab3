package lab3.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

final case class DataQualityMetrics(
  totalRows: Long,
  nullRecordIds: Long,
  nullOrderDates: Long,
  nullQuantities: Long,
  nullAmounts: Long,
  nullStates: Long,
  nullCities: Long,
  nullSkus: Long,
  duplicateOrderIds: Long,
  minimumDate: Option[String],
  maximumDate: Option[String],
  maximumSkuMonthRows: Long,
  skuMonthGroupsAbove1000: Long
)

object DataQuality {
  def collect(base: DataFrame): DataQualityMetrics = {
    val summary = base.agg(
      count(lit(1)).as("total_rows"),
      sum(when(col("record_id").isNull, 1L).otherwise(0L)).as("null_record_ids"),
      sum(when(col("order_date").isNull, 1L).otherwise(0L)).as("null_order_dates"),
      sum(when(col("qty").isNull, 1L).otherwise(0L)).as("null_quantities"),
      sum(when(col("amount").isNull, 1L).otherwise(0L)).as("null_amounts"),
      sum(when(col("state").isNull, 1L).otherwise(0L)).as("null_states"),
      sum(when(col("city").isNull, 1L).otherwise(0L)).as("null_cities"),
      sum(when(col("sku").isNull, 1L).otherwise(0L)).as("null_skus"),
      min(col("order_date")).as("minimum_date"),
      max(col("order_date")).as("maximum_date")
    ).first()

    val duplicateOrderIds = base.filter(col("order_id").isNotNull)
      .groupBy("order_id").count().filter(col("count") > 1L).count()

    val skuMonthSizes = base.filter(col("sku").isNotNull && col("month").isNotNull)
      .groupBy("sku", "month").count()
    val skuMonthSummary = skuMonthSizes.agg(
      coalesce(max(col("count")), lit(0L)).as("maximum_sku_month_rows"),
      sum(when(col("count") > 1000L, 1L).otherwise(0L)).as("groups_above_1000")
    ).first()

    DataQualityMetrics(
      totalRows = summary.getAs[Long]("total_rows"),
      nullRecordIds = summary.getAs[Long]("null_record_ids"),
      nullOrderDates = summary.getAs[Long]("null_order_dates"),
      nullQuantities = summary.getAs[Long]("null_quantities"),
      nullAmounts = summary.getAs[Long]("null_amounts"),
      nullStates = summary.getAs[Long]("null_states"),
      nullCities = summary.getAs[Long]("null_cities"),
      nullSkus = summary.getAs[Long]("null_skus"),
      duplicateOrderIds = duplicateOrderIds,
      minimumDate = dateString(summary, "minimum_date"),
      maximumDate = dateString(summary, "maximum_date"),
      maximumSkuMonthRows = skuMonthSummary.getAs[Long]("maximum_sku_month_rows"),
      skuMonthGroupsAbove1000 = Option(skuMonthSummary.getAs[java.lang.Long]("groups_above_1000")).fold(0L)(_.longValue())
    )
  }

  def invalidSample(base: DataFrame, limitRows: Int = 20): DataFrame = {
    require(limitRows > 0, "limitRows must be positive")
    base.filter(
      col("record_id").isNull || col("order_id").isNull || col("order_date").isNull || col("qty").isNull
    ).orderBy(col("record_id").asc_nulls_first).limit(limitRows)
  }

  private def dateString(row: Row, field: String): Option[String] =
    Option(row.getAs[java.sql.Date](field)).map(_.toLocalDate.toString)
}
