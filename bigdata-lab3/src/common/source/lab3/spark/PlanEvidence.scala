package lab3.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.DataFrame

object PlanEvidence {
  private val JoinNames = Seq(
    "BroadcastHashJoin",
    "SortMergeJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct"
  )

  def writeExtendedPlan(dataFrame: DataFrame, path: Path): String = {
    dataFrame.explain(extended = true)
    val plan = dataFrame.queryExecution.toString()
    Option(path.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    Files.write(path, plan.getBytes(StandardCharsets.UTF_8))
    plan
  }

  def executedPlan(dataFrame: DataFrame): String = dataFrame.queryExecution.executedPlan.toString()

  def countExchangeNodes(executedPlan: String): Int =
    Option(executedPlan).toSeq.flatMap(_.lines).count { line =>
      line.replaceFirst("^[|:+\\- ]+", "").startsWith("Exchange ")
    }

  def joinStrategies(executedPlan: String): Seq[String] =
    JoinNames.filter(name => Option(executedPlan).exists(_.contains(name)))
}
