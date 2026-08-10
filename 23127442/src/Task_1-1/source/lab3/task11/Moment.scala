package lab3.task11

final case class Moment(count: Long, amountCount: Long, sum: Double, sumSquares: Double) {
  require(count >= 0L, "count must be non-negative")
  require(amountCount >= 0L && amountCount <= count, "amountCount must be between 0 and count")

  def combine(other: Moment): Moment =
    Moment(
      count + other.count,
      amountCount + other.amountCount,
      sum + other.sum,
      sumSquares + other.sumSquares
    )

  def populationVariance: Option[Double] = {
    if (amountCount == 0L) None
    else {
      val mean = sum / amountCount.toDouble
      val raw = sumSquares / amountCount.toDouble - mean * mean
      Some(if (raw < 0.0 && raw > -1e-9) 0.0 else raw)
    }
  }
}

object Moment {
  val Empty: Moment = Moment(0L, 0L, 0.0, 0.0)

  def fromAmount(amount: Option[Double]): Moment = amount match {
    case Some(value) => Moment(1L, 1L, value, value * value)
    case None => Moment(1L, 0L, 0.0, 0.0)
  }
}
