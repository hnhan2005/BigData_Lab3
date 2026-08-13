package lab3.task22

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import lab3.common.{Cli, CsvEncoding}
import lab3.io.SingleFileExporter
import lab3.spark.{PlanEvidence, StageCollector}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task22Main {
  def main(args: Array[String]): Unit = {
    val exitCode = runArgs(args)
    if (exitCode != 0) sys.exit(exitCode)
  }

  def runArgs(args: Array[String]): Int = {
    Cli.parse(args) match {
      case Left(error) => fail(error)
      case Right(options) =>
        Cli.requireValues(options, Seq("input", "output-local", "evidence-dir")) match {
          case Left(error) => fail(error)
          case Right(_) =>
            (Cli.positiveInt(options, "accuracy", 10000), Cli.positiveInt(options, "runs", 5)) match {
              case (Left(error), _) => fail(error)
              case (_, Left(error)) => fail(error)
              case (Right(_), Right(runs)) if runs < 5 => fail("--runs phải ít nhất là 5")
              case (Right(accuracy), Right(runs)) => execute(options, accuracy, runs)
            }
        }
    }
  }

  private def execute(options: lab3.common.ParsedOptions, accuracy: Int, runs: Int): Int = {
    val spark = SparkSession.builder().appName("bigdata-lab3-task22").getOrCreate()
    try {
      val evidence = Paths.get(options.value("evidence-dir").get)
      Files.createDirectories(evidence)
      val base = Task22Job.base(spark, options.value("input").get).cache()
      base.count()
      val profile = Task22Pipeline.groupSizeProfile(base)
      writeText(evidence.resolve("group-profile.txt"), Seq(
        "valid_rows=" + profile.validRows,
        "group_count=" + profile.groupCount,
        "maximum_group_rows=" + profile.maximumGroupRows,
        "groups_above_1000=" + profile.groupsAbove1000,
        "estimated_bytes_per_row=" + profile.estimatedBytesPerRow,
        "estimated_maximum_group_bytes=" + profile.estimatedMaximumGroupBytes
      ).mkString("\n") + "\n")

      val approximateThresholds = Task22Pipeline.approximateThresholds(base, accuracy)
      val exactThresholds = Task22Pipeline.exactThresholds(base)
      val approximate = Task22Pipeline.qualifyingStatistics(base, approximateThresholds)
      val exact = Task22Pipeline.qualifyingStatistics(base, exactThresholds)
      val comparison = Task22Pipeline.compare(base, approximateThresholds, exactThresholds)
      writeEvidenceFrame(comparison.thresholdDeltas, evidence.resolve("threshold-deltas"), options.flag("overwrite"))
      writeEvidenceFrame(comparison.setDifferenceSummary, evidence.resolve("set-difference-summary"), options.flag("overwrite"))
      writeEvidenceFrame(comparison.boundedSetDifferenceExamples, evidence.resolve("set-difference-examples"), options.flag("overwrite"))

      val group = "lab3-task22-benchmark"
      val collector = new StageCollector(group)
      spark.sparkContext.addSparkListener(collector)
      spark.sparkContext.setJobGroup(group, "Task 2-2 fair benchmark")
      val benchmark = try BenchmarkHarness.compare(
        () => force(approximate),
        () => force(exact),
        warmups = 1,
        runs = runs
      ) finally spark.sparkContext.clearJobGroup()
      writeBenchmark(evidence, benchmark)

      val result = approximate.unionByName(exact).orderBy("sku", "month", "method", "percentile_level")
      PlanEvidence.writeExtendedPlan(result, evidence.resolve("extended-plan.txt"))
      SingleFileExporter.exportParquet(
        result,
        Paths.get(options.value("output-local").get),
        options.flag("overwrite")
      )
      val physical = PlanEvidence.executedPlan(result)
      writeText(evidence.resolve("execution-summary.txt"), Seq(
        "spark_version=" + spark.version,
        "master=" + spark.sparkContext.master,
        "accuracy=" + accuracy,
        "runs=" + runs,
        "exchange_count=" + PlanEvidence.countExchangeNodes(physical),
        "join_strategies=" + PlanEvidence.joinStrategies(physical).mkString(","),
        "benchmark_stage_ids=" + collector.stageIds.toSeq.sorted.mkString(",")
      ).mkString("\n") + "\n")
      base.unpersist()
      0
    } catch {
      case error: Exception => fail(error.getMessage)
    } finally spark.stop()
  }

  private def force(dataFrame: DataFrame): Unit = dataFrame.foreachPartition(iterator => while (iterator.hasNext) iterator.next())

  private def writeEvidenceFrame(dataFrame: DataFrame, path: Path, overwrite: Boolean): Unit =
    dataFrame.coalesce(1).write.mode(if (overwrite) "overwrite" else "error").option("header", "true").csv(path.toString)

  private def writeBenchmark(directory: Path, result: BenchmarkResult): Unit = {
    val samples = "method,run,elapsed_ms\n" + result.samples.map { sample =>
      CsvEncoding.row(Seq(sample.method, sample.run, sample.elapsedMs))
    }.mkString("\n") + "\n"
    val summaries = "method,runs,mean_ms,sample_stddev_ms\n" + result.summaries.map { summary =>
      CsvEncoding.row(Seq(summary.method, summary.runs, summary.meanMs, summary.sampleStddevMs))
    }.mkString("\n") + "\n"
    writeText(directory.resolve("benchmark-samples.csv"), samples)
    writeText(directory.resolve("benchmark-summary.csv"), summaries)
  }

  private def writeText(path: Path, content: String): Unit = {
    Option(path.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def fail(message: String): Int = {
    System.err.println("[ERROR] " + message)
    System.err.println(
      "Usage: Task22Main --input <csv-path> --output-local <Task_2-2.parquet> --evidence-dir <dir> [--accuracy 10000] [--runs 5] [--overwrite]"
    )
    2
  }
}
