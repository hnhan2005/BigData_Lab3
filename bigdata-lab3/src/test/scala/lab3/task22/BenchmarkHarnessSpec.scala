package lab3.task22

import org.scalatest.{FlatSpec, Matchers}

class BenchmarkHarnessSpec extends FlatSpec with Matchers {
  "BenchmarkHarness.summarize" should "calculate arithmetic mean and sample standard deviation" in {
    val samples = Vector(10L, 20L, 30L, 40L, 50L).zipWithIndex.map {
      case (elapsed, index) => BenchmarkSample("exact", index + 1, elapsed)
    }
    val summary = BenchmarkHarness.summarize(samples)
    summary.meanMs shouldBe 30.0
    summary.sampleStddevMs shouldBe (math.sqrt(250.0) +- 1e-12)
    summary.runs shouldBe 5
  }

  "BenchmarkHarness.compare" should "reject fewer than five measured runs" in {
    an[IllegalArgumentException] should be thrownBy BenchmarkHarness.compare(() => (), () => (), runs = 4)
  }

  it should "record five samples for each method" in {
    val result = BenchmarkHarness.compare(() => (), () => (), warmups = 0, runs = 5)
    result.samples.count(_.method == "approx") shouldBe 5
    result.samples.count(_.method == "exact") shouldBe 5
    result.summaries.map(_.runs) shouldBe Vector(5, 5)
  }
}
