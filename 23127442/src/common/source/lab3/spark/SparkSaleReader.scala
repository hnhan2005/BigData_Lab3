package lab3.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkSaleReader {
  val RawSchema: StructType = StructType(
    Seq(
      "index",
      "Order ID",
      "Date",
      "Status",
      "Fulfilment",
      "Sales Channel ",
      "ship-service-level",
      "Style",
      "SKU",
      "Category",
      "Size",
      "ASIN",
      "Courier Status",
      "Qty",
      "currency",
      "Amount",
      "ship-city",
      "ship-state",
      "ship-postal-code",
      "ship-country",
      "promotion-ids",
      "B2B",
      "fulfilled-by",
      "Unnamed: 22"
    ).map(name => StructField(name, StringType, nullable = true))
  )

  def read(spark: SparkSession, input: String): DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val raw = spark.read
      .schema(RawSchema)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "false")
      .csv(input)

    raw.select(
      col("index").cast(LongType).as("record_id"),
      clean(col("Order ID")).as("order_id"),
      to_date(trim(col("Date")), "MM-dd-yy").as("order_date"),
      normalized(col("Status")).as("status"),
      normalized(col("Fulfilment")).as("fulfilment"),
      normalized(col("ship-service-level")).as("service_level"),
      normalized(col("Style")).as("style"),
      normalized(col("SKU")).as("sku"),
      normalized(col("Size")).as("size"),
      normalized(col("Courier Status")).as("courier_status"),
      col("Qty").cast(LongType).as("qty"),
      col("Amount").cast(DoubleType).as("amount"),
      normalized(col("ship-city")).as("city"),
      normalized(col("ship-state")).as("state"),
      col("promotion-ids").as("promotion_ids_raw")
    ).withColumn("month", date_format(col("order_date"), "yyyy-MM"))
      .withColumn("is_bought", col("qty") > lit(0L) && col("status").contains("SHIPPED"))
  }

  private def clean(value: Column): Column = {
    val cleaned = trim(value)
    when(length(cleaned) > 0, cleaned)
  }

  private def normalized(value: Column): Column = {
    val cleaned = upper(trim(regexp_replace(value, "\\s+", " ")))
    when(length(cleaned) > 0, cleaned)
  }
}
