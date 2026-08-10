package lab3.task22

final case class BenchmarkSample(method: String, run: Int, elapsedMs: Long)
final case class BenchmarkSummary(method: String, runs: Int, meanMs: Double, sampleStddevMs: Double)
final case class BenchmarkResult(samples: Vector[BenchmarkSample], summaries: Vector[BenchmarkSummary])

object BenchmarkHarness {
  def compare(
    approximateAction: () => Unit,
    exactAction: () => Unit,
    warmups: Int = 1,
    runs: Int = 5
  ): BenchmarkResult = {
    require(warmups >= 0, "warmups must be non-negative")
    require(runs >= 5, "runs must be at least 5")

    (1 to warmups).foreach { _ =>
      approximateAction()
      exactAction()
    }

    val actions = Map("approx" -> approximateAction, "exact" -> exactAction)
    val samples = (1 to runs).flatMap { run =>
      val order = if (run % 2 == 1) Seq("approx", "exact") else Seq("exact", "approx")
      order.map { method =>
        val started = System.nanoTime()
        actions(method)()
        val elapsedMs = math.max(0L, (System.nanoTime() - started) / 1000000L)
        BenchmarkSample(method, run, elapsedMs)
      }
    }.toVector

    BenchmarkResult(samples, Seq("approx", "exact").map(method => summarize(samples.filter(_.method == method))).toVector)
  }

  def summarize(samples: Seq[BenchmarkSample]): BenchmarkSummary = {
    require(samples.nonEmpty, "samples must not be empty")
    val methods = samples.map(_.method).distinct
    require(methods.size == 1, "samples must belong to one method")
    val values = samples.map(_.elapsedMs.toDouble)
    val mean = values.sum / values.size
    val variance =
      if (values.size < 2) 0.0
      else values.map(value => math.pow(value - mean, 2.0)).sum / (values.size - 1)
    BenchmarkSummary(methods.head, values.size, mean, math.sqrt(variance))
  }
}
