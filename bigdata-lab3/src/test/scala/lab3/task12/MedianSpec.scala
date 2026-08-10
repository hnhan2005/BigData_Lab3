package lab3.task12

import org.scalatest.{FlatSpec, Matchers}

class MedianSpec extends FlatSpec with Matchers {
  "Median.exact" should "calculate odd and even exact medians without mutating input" in {
    val odd = Vector(9L, 1L, 5L)
    Median.exact(odd) shouldBe Right(5.0)
    odd shouldBe Vector(9L, 1L, 5L)
    Median.exact(Vector(10L, 2L, 4L, 8L)) shouldBe Right(6.0)
  }

  it should "return a typed failure for an empty group" in {
    Median.exact(Vector.empty) shouldBe Left("Không thể tính median cho tập rỗng")
  }

  it should "avoid Long overflow for an even pair" in {
    Median.exact(Vector(Long.MaxValue, Long.MaxValue)) shouldBe Right(Long.MaxValue.toDouble)
  }
}
