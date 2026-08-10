package lab3.task12

import lab3.common.Normalization
import org.scalatest.{FlatSpec, Matchers}

class Task12LogicSpec extends FlatSpec with Matchers {
  "StyleVariety.qualifyingVariety" should "count distinct SKU only for a style containing XXL or larger" in {
    StyleVariety.qualifyingVariety(
      Iterator("SKU-A" -> false, "SKU-A" -> true, "SKU-B" -> false)
    ) shouldBe Some(2L)
    StyleVariety.qualifyingVariety(Iterator("SKU-A" -> false, "SKU-B" -> false)) shouldBe None
    StyleVariety.qualifyingVariety(Iterator.empty) shouldBe None
  }

  "Normalization.isAtLeastXXL" should "cover required qualifying and non-qualifying aliases" in {
    Seq("XXL", "2XL", "XXXL", "3XL", "3 XL", "6XL").foreach { size =>
      withClue(size) { Normalization.isAtLeastXXL(size) shouldBe true }
    }
    Seq("XL", "Free", "unknown", "").foreach { size =>
      withClue(size) { Normalization.isAtLeastXXL(size) shouldBe false }
    }
  }

  "Task12Keys" should "round-trip style, state-month, and SKU-size values" in {
    Task12Keys.parseStyle(Task12Keys.style("STATE", "2022-04", "STYLE")) shouldBe
      (("STATE", "2022-04", "STYLE"))
    Task12Keys.parseStateMonth(Task12Keys.stateMonth("STATE", "2022-04")) shouldBe (("STATE", "2022-04"))
    Task12Keys.parseSkuAndSize(Task12Keys.skuAndSize("SKU", atLeastXXL = true)) shouldBe (("SKU", true))
  }

  "Sale month" should "remain separate across a calendar-month boundary" in {
    Task12Keys.stateMonth("STATE", "2022-04") should not be Task12Keys.stateMonth("STATE", "2022-05")
  }
}
