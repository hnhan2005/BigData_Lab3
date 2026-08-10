package lab3.io

import java.nio.file.Paths

import lab3.common.Cli
import org.apache.spark.sql.SparkSession

object ValidationMain {
  def main(args: Array[String]): Unit = {
    val options = Cli.parse(args).fold(error => throw new IllegalArgumentException(error), identity)
    val output = Cli.requireValues(options, Seq("output-dir"))
      .fold(error => throw new IllegalArgumentException(error), _.value("output-dir").get)
    val root = Paths.get(output)
    OutputValidator.validateTask11Csv(root.resolve("Task_1-1.csv"))
    OutputValidator.validateTask12Csv(root.resolve("Task_1-2.csv"))
    val spark = SparkSession.builder().appName("bigdata-lab3-output-validation").getOrCreate()
    try {
      OutputValidator.validateTask21Parquet(spark, root.resolve("Task_2-1.parquet").toString)
      OutputValidator.validateTask22Parquet(spark, root.resolve("Task_2-2.parquet").toString)
    } finally spark.stop()
    println("[OK] Bốn output đã qua schema, key và invariant validation")
  }
}
