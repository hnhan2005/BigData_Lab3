package lab3.common

import java.net.URI

final case class ParsedOptions(values: Map[String, String], flags: Set[String]) {
  def value(name: String): Option[String] = values.get(name)
  def flag(name: String): Boolean = flags.contains(name)
}

object Cli {
  def parse(args: Array[String]): Either[String, ParsedOptions] = {
    var values = Map.empty[String, String]
    var flags = Set.empty[String]
    var index = 0

    while (index < args.length) {
      val token = args(index)
      if (!token.startsWith("--") || token.length == 2) {
        return Left("Tham số không hợp lệ: " + token)
      }

      val name = token.substring(2)
      if (values.contains(name) || flags.contains(name)) {
        return Left("Tham số bị lặp: --" + name)
      }

      if (index + 1 < args.length && !args(index + 1).startsWith("--")) {
        values += name -> args(index + 1)
        index += 2
      } else {
        flags += name
        index += 1
      }
    }

    Right(ParsedOptions(values, flags))
  }

  def requireValues(options: ParsedOptions, names: Seq[String]): Either[String, ParsedOptions] = {
    val missing = names.filter(name => options.value(name).forall(_.trim.isEmpty))
    if (missing.isEmpty) Right(options)
    else Left("Thiếu tham số bắt buộc: " + missing.map("--" + _).mkString(", "))
  }

  def positiveInt(options: ParsedOptions, name: String, default: Int): Either[String, Int] = {
    options.value(name) match {
      case None => Right(default)
      case Some(raw) =>
        try {
          val value = raw.toInt
          if (value > 0) Right(value)
          else Left("--" + name + " phải lớn hơn 0")
        } catch {
          case _: NumberFormatException => Left("--" + name + " phải là số nguyên")
        }
    }
  }

  def uri(options: ParsedOptions, name: String, schemes: Set[String]): Either[String, URI] = {
    options.value(name) match {
      case None => Left("Thiếu tham số bắt buộc: --" + name)
      case Some(raw) =>
        try {
          val parsed = new URI(raw)
          if (parsed.getScheme == null || !schemes.contains(parsed.getScheme.toLowerCase)) {
            Left("--" + name + " phải dùng URI scheme: " + schemes.toSeq.sorted.mkString(", "))
          } else {
            Right(parsed)
          }
        } catch {
          case _: Exception => Left("--" + name + " không phải URI hợp lệ")
        }
    }
  }

  def validateOutputState(exists: Boolean, overwrite: Boolean): Either[String, Unit] = {
    if (exists && !overwrite) Left("Output đã tồn tại; dùng --overwrite để thay thế")
    else Right(())
  }
}
