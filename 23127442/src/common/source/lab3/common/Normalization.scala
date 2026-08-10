package lab3.common

import java.util.Locale

object Normalization {
  private val NumericXl = "^([2-9]|[1-9][0-9]+)XL$".r
  private val RepeatedXl = "^X{2,}L$".r

  def normalizeDimension(value: String): Option[String] =
    Option(value)
      .map(_.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT))
      .filter(_.nonEmpty)

  def normalizedEquals(value: String, expected: String): Boolean =
    normalizeDimension(value).contains(expected.toUpperCase(Locale.ROOT))

  def isBought(status: String, qty: Long): Boolean =
    qty != 0L && normalizeDimension(status).exists(_.contains("SHIPPED"))

  def isAtLeastXXL(size: String): Boolean =
    normalizeDimension(size)
      .map(_.replace(" ", ""))
      .exists {
        case NumericXl(_) => true
        case RepeatedXl() => true
        case _ => false
      }

  def parsePromotions(raw: String): Vector[String] =
    Option(raw)
      .toVector
      .flatMap(_.split(",", -1))
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
}
