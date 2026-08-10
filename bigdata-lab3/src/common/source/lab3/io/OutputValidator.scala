package lab3.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object OutputValidator {
  val Task11Header = Vector("state", "window_date", "window_days", "winning_size", "frequency", "population_variance")
  val Task12Header = Vector("state", "month", "median_variety", "qualifying_style_count")

  def validateTask11Csv(path: Path): Unit = {
    val rows = readCsv(path, Task11Header)
    requireUnique(rows.map(row => row("state") -> row("window_date")), "Task 1-1 state/window_date")
    rows.foreach { row =>
      require(Set(5, 10).contains(row("window_days").toInt), "window_days phải là 5 hoặc 10")
      require(row("frequency").toLong > 0L, "frequency phải dương")
      Option(row("population_variance")).filter(_.nonEmpty).foreach { raw =>
        val value = raw.toDouble
        require(value >= 0.0 && !value.isNaN && !value.isInfinity, "variance không hợp lệ")
      }
    }
  }

  def validateTask12Csv(path: Path): Unit = {
    val rows = readCsv(path, Task12Header)
    requireUnique(rows.map(row => row("state") -> row("month")), "Task 1-2 state/month")
    rows.foreach { row =>
      require(row("month").matches("\\d{4}-\\d{2}"), "month không đúng yyyy-MM")
      require(row("median_variety").toDouble >= 1.0, "median_variety phải >= 1")
      require(row("qualifying_style_count").toLong > 0L, "qualifying_style_count phải dương")
    }
  }

  def validateTask21Parquet(spark: SparkSession, path: String): Unit = {
    val frame = spark.read.parquet(path)
    requireSchema(frame, Vector(
      "state" -> StringType, "city" -> StringType, "cancelled_standard_orders" -> LongType,
      "qualifying_orders" -> LongType, "percentage" -> DoubleType
    ))
    requireNoDuplicateKeys(frame, Seq("state", "city"))
    val invalid = frame.filter(
      col("state").isNull || col("city").isNull || col("cancelled_standard_orders") <= 0L ||
        col("qualifying_orders") < 0L || col("qualifying_orders") > col("cancelled_standard_orders") ||
        col("percentage").isNull || isnan(col("percentage")) ||
        col("percentage") < 0.0 || col("percentage") > 100.0
    ).limit(1).count()
    require(invalid == 0L, "Task 2-1 vi phạm count/percentage invariant")
  }

  def validateTask22Parquet(spark: SparkSession, path: String): Unit = {
    val frame = spark.read.parquet(path)
    requireSchema(frame, Vector(
      "sku" -> StringType, "month" -> StringType, "method" -> StringType, "percentile_level" -> StringType,
      "threshold" -> DoubleType, "qualifying_order_count" -> LongType, "amount_value_count" -> LongType,
      "amount_stddev_pop" -> DoubleType
    ))
    requireNoDuplicateKeys(frame, Seq("sku", "month", "method", "percentile_level"))
    val wrongMultiplicity = frame.groupBy("sku", "month").count().filter(col("count") =!= 4L).limit(1).count()
    require(wrongMultiplicity == 0L, "Mỗi SKU-month phải có đúng 4 rows")
    val invalid = frame.filter(
      col("sku").isNull || col("month").isNull || !col("method").isin("approx", "exact") ||
        !col("percentile_level").isin("P80", "P90") || col("threshold").isNull || col("threshold") < 0.0 ||
        col("qualifying_order_count") <= 0L || col("amount_value_count") < 0L ||
        col("amount_value_count") > col("qualifying_order_count") || col("amount_stddev_pop").isNull ||
        isnan(col("amount_stddev_pop")) || col("amount_stddev_pop") < 0.0
    ).limit(1).count()
    require(invalid == 0L, "Task 2-2 vi phạm schema/value invariant")
  }

  private def readCsv(path: Path, expectedHeader: Vector[String]): Vector[Map[String, String]] = {
    require(Files.isRegularFile(path), "Không tìm thấy CSV: " + path)
    val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)
    val parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)
    try {
      val actualHeader = parser.getHeaderMap.keySet().asScala.toVector
      require(actualHeader == expectedHeader, "CSV header không khớp: " + actualHeader.mkString(","))
      parser.iterator().asScala.map { record =>
        expectedHeader.map(name => name -> record.get(name)).toMap
      }.toVector
    } finally {
      parser.close()
      reader.close()
    }
  }

  private def requireSchema(frame: DataFrame, expected: Vector[(String, DataType)]): Unit = {
    val actual = frame.schema.fields.map(field => field.name -> field.dataType).toVector
    require(actual == expected, "Parquet schema không khớp: " + actual.mkString(","))
  }

  private def requireNoDuplicateKeys(frame: DataFrame, keys: Seq[String]): Unit = {
    val duplicate = frame.groupBy(keys.map(col): _*).count().filter(col("count") =!= 1L).limit(1).count()
    require(duplicate == 0L, "Trùng logical key: " + keys.mkString(","))
  }

  private def requireUnique[A](keys: Seq[A], label: String): Unit =
    require(keys.distinct.size == keys.size, "Trùng logical key " + label)
}
