package lab3.task11

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.util.Base64

object Task11Keys {
  private val Separator = "\u0001"

  def bucket(state: String, date: LocalDate, size: String): String =
    Seq(state, date.toString, size).mkString(Separator)

  def parseBucket(value: String): (String, String, String) = {
    val parts = value.split(Separator, -1)
    require(parts.length == 3, "Invalid bucket key")
    (parts(0), parts(1), parts(2))
  }

  def window(state: String, date: String): String = Seq(state, date).mkString(Separator)

  def parseWindow(value: String): (String, String) = {
    val parts = value.split(Separator, -1)
    require(parts.length == 2, "Invalid window key")
    (parts(0), parts(1))
  }

  def candidate(candidate: SizeCandidate): String = {
    val moment = candidate.moment
    Seq(
      candidate.size,
      moment.count.toString,
      moment.amountCount.toString,
      moment.sum.toString,
      moment.sumSquares.toString
    ).mkString(Separator)
  }

  def parseCandidate(value: String): SizeCandidate = {
    val parts = value.split(Separator, -1)
    require(parts.length == 5, "Invalid candidate value")
    SizeCandidate(parts(0), Moment(parts(1).toLong, parts(2).toLong, parts(3).toDouble, parts(4).toDouble))
  }
}

object WindowConfig {
  val Key = "lab3.task11.state-windows"

  def encode(windows: Map[String, Int]): String =
    windows.toSeq.sortBy(_._1).map {
      case (state, days) =>
        Base64.getEncoder.encodeToString(state.getBytes(StandardCharsets.UTF_8)) + ":" + days
    }.mkString(",")

  def decode(raw: String): Map[String, Int] = {
    Option(raw).filter(_.nonEmpty).toSeq.flatMap(_.split(",", -1)).map { entry =>
      val parts = entry.split(":", 2)
      require(parts.length == 2, "Invalid state-window entry")
      new String(Base64.getDecoder.decode(parts(0)), StandardCharsets.UTF_8) -> parts(1).toInt
    }.toMap
  }
}

object WindowLength {
  def days(totalBoughtRows: Long): Int = if (totalBoughtRows > 10000L) 5 else 10
}

object WindowBuckets {
  def dates(purchaseDate: LocalDate, days: Int): Vector[LocalDate] = {
    require(days > 0, "window days must be positive")
    (1 to days).map(offset => purchaseDate.plusDays(offset.toLong)).toVector
  }
}
