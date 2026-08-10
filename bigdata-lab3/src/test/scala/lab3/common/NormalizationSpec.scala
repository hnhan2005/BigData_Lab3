package lab3.common

import org.scalatest.{FlatSpec, Matchers}

class NormalizationSpec extends FlatSpec with Matchers {
  "Normalization.normalizeDimension" should "trim, collapse whitespace, and uppercase" in {
    Normalization.normalizeDimension("  Tamil   Nadu  ") shouldBe Some("TAMIL NADU")
    Normalization.normalizeDimension("  ") shouldBe None
    Normalization.normalizeDimension(null) shouldBe None
  }

  "Normalization.isBought" should "require shipped in status and non-zero quantity" in {
    Normalization.isBought("Shipped", 1L) shouldBe true
    Normalization.isBought("Shipped - Delivered to Buyer", -1L) shouldBe true
    Normalization.isBought("Shipped", 0L) shouldBe false
    Normalization.isBought("Shipping", 1L) shouldBe false
    Normalization.isBought("Cancelled", 1L) shouldBe false
  }

  "Normalization.isAtLeastXXL" should "recognize textual and numeric XXL aliases" in {
    Seq("XXL", "2XL", "XXXL", "3XL", "4XL", "6XL", "10XL").foreach { size =>
      withClue(size) { Normalization.isAtLeastXXL(size) shouldBe true }
    }
    Seq("XL", "L", "Free", "unknown", "").foreach { size =>
      withClue(size) { Normalization.isAtLeastXXL(size) shouldBe false }
    }
  }

  "Normalization.parsePromotions" should "preserve first-seen order and remove blanks and duplicates" in {
    Normalization.parsePromotions(" Amazon Promo, Promo X,Amazon Promo, ,Promo Y ") shouldBe
      Vector("Amazon Promo", "Promo X", "Promo Y")
    Normalization.parsePromotions(null) shouldBe empty
  }
}
