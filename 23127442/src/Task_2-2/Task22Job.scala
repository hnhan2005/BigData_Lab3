package lab3.task22

import lab3.spark.SparkSaleReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task22Job {
  def base(spark: SparkSession, input: String): DataFrame =
    Task22Pipeline.skuMonthBase(SparkSaleReader.read(spark, input))

  def buildApprox(base: DataFrame, accuracy: Int): DataFrame =
    Task22Pipeline.qualifyingStatistics(base, Task22Pipeline.approximateThresholds(base, accuracy))

  def buildExact(base: DataFrame): DataFrame =
    Task22Pipeline.qualifyingStatistics(base, Task22Pipeline.exactThresholds(base))

  def buildFinal(base: DataFrame, accuracy: Int): DataFrame =
    buildApprox(base, accuracy).unionByName(buildExact(base))
      .orderBy("sku", "month", "method", "percentile_level")
}
