package lab3.task11

final case class SizeCandidate(size: String, moment: Moment)

object Winner {
  def choose(candidates: Iterable[SizeCandidate]): Option[SizeCandidate] =
    candidates.reduceOption(better)

  def better(left: SizeCandidate, right: SizeCandidate): SizeCandidate = {
    if (left.moment.count != right.moment.count) {
      if (left.moment.count > right.moment.count) left else right
    } else {
      compareVariance(left.moment.populationVariance, right.moment.populationVariance) match {
        case value if value < 0 => left
        case value if value > 0 => right
        case _ => if (left.size.compareTo(right.size) <= 0) left else right
      }
    }
  }

  private def compareVariance(left: Option[Double], right: Option[Double]): Int =
    (left, right) match {
      case (Some(a), Some(b)) => java.lang.Double.compare(a, b)
      case (Some(_), None) => -1
      case (None, Some(_)) => 1
      case (None, None) => 0
    }
}
