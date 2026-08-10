package lab3.task11

import java.time.LocalDate

import lab3.common.CsvEncoding
import org.scalatest.{FlatSpec, Matchers}

class Task11LogicSpec extends FlatSpec with Matchers {
  "WindowLength.days" should "use 10 days at 10000 and 5 days strictly above it" in {
    WindowLength.days(0L) shouldBe 10
    WindowLength.days(10000L) shouldBe 10
    WindowLength.days(10001L) shouldBe 5
  }

  "WindowBuckets.dates" should "emit t+1 through t+w including unseen future dates" in {
    val purchaseDate = LocalDate.of(2022, 6, 29)
    WindowBuckets.dates(purchaseDate, 5) shouldBe Vector(
      LocalDate.of(2022, 6, 30),
      LocalDate.of(2022, 7, 1),
      LocalDate.of(2022, 7, 2),
      LocalDate.of(2022, 7, 3),
      LocalDate.of(2022, 7, 4)
    )
  }

  "Moment" should "combine associatively and compute population variance" in {
    val a = Moment.fromAmount(Some(1.0))
    val b = Moment.fromAmount(Some(2.0))
    val c = Moment.fromAmount(Some(3.0))
    a.combine(b).combine(c) shouldBe a.combine(b.combine(c))
    a.combine(b).combine(c).populationVariance.get shouldBe (2.0 / 3.0 +- 1e-12)
  }

  it should "count rows with missing amount while leaving variance undefined" in {
    val missing = Moment.fromAmount(None)
    missing.count shouldBe 1L
    missing.amountCount shouldBe 0L
    missing.populationVariance shouldBe None
  }

  "Winner" should "apply frequency, lower variance, and lexical tie-breaks in order" in {
    val frequent = SizeCandidate("ZZ", Moment(3L, 3L, 6.0, 14.0))
    val lessFrequent = SizeCandidate("AA", Moment(2L, 2L, 2.0, 2.0))
    Winner.better(frequent, lessFrequent) shouldBe frequent

    val lowVariance = SizeCandidate("XL", Moment(2L, 2L, 10.0, 50.0))
    val highVariance = SizeCandidate("L", Moment(2L, 2L, 10.0, 58.0))
    Winner.better(lowVariance, highVariance) shouldBe lowVariance

    val lexicalL = SizeCandidate("L", Moment(2L, 2L, 10.0, 50.0))
    Winner.better(lowVariance, lexicalL) shouldBe lexicalL
  }

  it should "rank an undefined variance after a finite variance when frequency ties" in {
    val undefined = SizeCandidate("A", Moment(2L, 0L, 0.0, 0.0))
    val finite = SizeCandidate("Z", Moment(2L, 1L, 5.0, 25.0))
    Winner.better(undefined, finite) shouldBe finite
  }

  "WindowConfig" should "round-trip states containing punctuation and Unicode" in {
    val windows = Map("TAMIL NADU" -> 5, "ĐÀ NẴNG: TEST" -> 10)
    WindowConfig.decode(WindowConfig.encode(windows)) shouldBe windows
  }

  "Task11Keys" should "round-trip bucket and candidate contracts" in {
    val date = LocalDate.of(2022, 4, 2)
    Task11Keys.parseBucket(Task11Keys.bucket("STATE", date, "XXL")) shouldBe ("STATE", "2022-04-02", "XXL")
    val candidate = SizeCandidate("XXL", Moment(2L, 1L, 100.0, 10000.0))
    Task11Keys.parseCandidate(Task11Keys.candidate(candidate)) shouldBe candidate
  }

  "CsvEncoding" should "quote commas, quotes, and line breaks" in {
    CsvEncoding.row(Seq("STATE, ONE", "A\"B", 2)) shouldBe "\"STATE, ONE\",\"A\"\"B\",2"
  }
}
