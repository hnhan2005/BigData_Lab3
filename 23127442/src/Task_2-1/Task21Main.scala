package lab3.task21

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import lab3.common.Cli
import lab3.io.SingleFileExporter
import lab3.spark.{PlanEvidence, StageCollector}
import org.apache.spark.sql.SparkSession

object Task21Main {
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
            val spark = SparkSession.builder().appName("bigdata-lab3-task21").getOrCreate()
            try {
              val result = Task21Job.build(spark, options.value("input").get)
              val evidenceDirectory = Paths.get(options.value("evidence-dir").get)
              Files.createDirectories(evidenceDirectory)
              val group = "lab3-task21"
              val collector = new StageCollector(group)
              spark.sparkContext.addSparkListener(collector)
              PlanEvidence.writeExtendedPlan(result, evidenceDirectory.resolve("extended-plan.txt"))
              spark.sparkContext.setJobGroup(group, "Task 2-1 final Parquet action")
              try {
                SingleFileExporter.exportParquet(
                  result,
                  Paths.get(options.value("output-local").get),
                  options.flag("overwrite")
                )
              } finally spark.sparkContext.clearJobGroup()

              val physical = PlanEvidence.executedPlan(result)
              val deadline = System.currentTimeMillis() + 5000L
              while (collector.stageIds.isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(25L)
              val summary = Seq(
                "job_group=" + group,
                "master=" + spark.sparkContext.master,
                "spark_version=" + spark.version,
                "join_strategies=" + PlanEvidence.joinStrategies(physical).mkString(","),
                "exchange_count=" + PlanEvidence.countExchangeNodes(physical),
                "stage_ids=" + collector.stageIds.toSeq.sorted.mkString(",")
              ).mkString("\n") + "\n"
              Files.write(evidenceDirectory.resolve("execution-summary.txt"), summary.getBytes(StandardCharsets.UTF_8))
              0
            } catch {
              case error: Exception => fail(error.getMessage)
            } finally spark.stop()
        }
    }
  }

  private def fail(message: String): Int = {
    System.err.println("[ERROR] " + message)
    System.err.println(
      "Usage: Task21Main --input <csv-path> --output-local <Task_2-1.parquet> --evidence-dir <dir> [--overwrite]"
    )
    2
  }
}
