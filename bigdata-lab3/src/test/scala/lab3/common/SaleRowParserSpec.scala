package lab3.common

import java.time.LocalDate

import org.scalatest.{FlatSpec, Matchers}

import scala.io.{Codec, Source}

class SaleRowParserSpec extends FlatSpec with Matchers {
  private def resourceLines(name: String): Vector[String] = {
    val url = getClass.getResource("/fixtures/" + name)
    val source = Source.fromURL(url)(Codec.UTF8)
    try source.getLines().toVector finally source.close()
  }

  "SaleRowParser" should "validate the exact supplied header" in {
    val header = resourceLines("shared-sales.csv").head
    SaleRowParser.validateHeader(header) shouldBe Right(())
    SaleRowParser.validateHeader("index,Order ID") shouldBe
      Left(StructuralDataError("CSV header không khớp schema 24 cột bắt buộc"))
  }

  it should "parse quoted commas, strict dates, nulls, and distinct promotions" in {
    val lines = resourceLines("shared-sales.csv")
    val first = SaleRowParser.parseCsvRecord(lines(1)).right.get
    first.recordId shouldBe 0L
    first.date shouldBe LocalDate.of(2022, 4, 1)
    first.month shouldBe "2022-04"
    first.city shouldBe Some("City, A")
    first.amount shouldBe Some(100.0)
    first.promotionIds shouldBe Vector("Amazon Promo", "Promo X")

    val second = SaleRowParser.parseCsvRecord(lines(2)).right.get
    second.amount shouldBe None
    second.promotionIds shouldBe empty

    val third = SaleRowParser.parseCsvRecord(lines(3)).right.get
    third.amount shouldBe None
    third.qty shouldBe 2L
  }

  it should "return deterministic typed errors" in {
    val invalid = resourceLines("invalid-sales.csv")(1)
    SaleRowParser.parseCsvRecord(invalid).left.get.field shouldBe "index"

    val badDate = invalid.replaceFirst("^bad", "3")
    SaleRowParser.parseCsvRecord(badDate).left.get.field shouldBe "Date"

    SaleRowParser.parseCsvRecord("1,too-short") shouldBe
      Left(StructuralDataError("CSV record có 2 cột; cần 24"))
  }

  it should "reject non-finite amounts" in {
    val valid = resourceLines("shared-sales.csv")(1)
    val nonFinite = valid.replace(",100.0,\"City, A\"", ",NaN,\"City, A\"")
    val error = SaleRowParser.parseCsvRecord(nonFinite).left.get
    error.field shouldBe "Amount"
  }
}
