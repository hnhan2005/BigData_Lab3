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
              val defaultGroup = "lab3-task21-default"
              val defaultCollector = new StageCollector(defaultGroup)
              spark.sparkContext.addSparkListener(defaultCollector)
              PlanEvidence.writeExtendedPlan(result, evidenceDirectory.resolve("extended-plan.txt"))
              spark.sparkContext.setJobGroup(defaultGroup, "Task 2-1 final Parquet action")
              try {
                SingleFileExporter.exportParquet(
                  result,
                  Paths.get(options.value("output-local").get),
                  options.flag("overwrite")
                )
              } finally spark.sparkContext.clearJobGroup()

              val defaultPhysical = PlanEvidence.executedPlan(result)
              val originalBroadcastThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
              spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1L)
              val noBroadcastResult = Task21Job.build(spark, options.value("input").get)
              val noBroadcastGroup = "lab3-task21-no-broadcast"
              val noBroadcastCollector = new StageCollector(noBroadcastGroup)
              spark.sparkContext.addSparkListener(noBroadcastCollector)
              PlanEvidence.writeExtendedPlan(
                noBroadcastResult,
                evidenceDirectory.resolve("extended-plan-no-broadcast.txt")
              )
              spark.sparkContext.setJobGroup(noBroadcastGroup, "Task 2-1 no-broadcast evidence action")
              try noBroadcastResult.foreachPartition(_ => ())
              finally {
                spark.sparkContext.clearJobGroup()
                spark.conf.set("spark.sql.autoBroadcastJoinThreshold", originalBroadcastThreshold)
              }
              val noBroadcastPhysical = PlanEvidence.executedPlan(noBroadcastResult)

              val deadline = System.currentTimeMillis() + 5000L
              while (
                (defaultCollector.stageIds.isEmpty || noBroadcastCollector.stageIds.isEmpty) &&
                  System.currentTimeMillis() < deadline
              ) Thread.sleep(25L)
              val summary = Seq(
                "master=" + spark.sparkContext.master,
                "spark_version=" + spark.version,
                "default_job_group=" + defaultGroup,
                "default_join_strategies=" + PlanEvidence.joinStrategies(defaultPhysical).mkString(","),
                "default_exchange_count=" + PlanEvidence.countExchangeNodes(defaultPhysical),
                "default_sort_count=" + PlanEvidence.countSortNodes(defaultPhysical),
                "default_stage_ids=" + defaultCollector.stageIds.toSeq.sorted.mkString(","),
                "no_broadcast_job_group=" + noBroadcastGroup,
                "no_broadcast_join_strategies=" + PlanEvidence.joinStrategies(noBroadcastPhysical).mkString(","),
                "no_broadcast_exchange_count=" + PlanEvidence.countExchangeNodes(noBroadcastPhysical),
                "no_broadcast_sort_count=" + PlanEvidence.countSortNodes(noBroadcastPhysical),
                "no_broadcast_stage_ids=" + noBroadcastCollector.stageIds.toSeq.sorted.mkString(",")
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
