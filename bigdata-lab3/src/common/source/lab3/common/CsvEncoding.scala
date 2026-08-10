package lab3.common

object CsvEncoding {
  def row(values: Seq[Any]): String = values.map(value => escape(String.valueOf(value))).mkString(",")

  def escape(value: String): String = {
    val safe = Option(value).getOrElse("")
    if (safe.exists(character => character == ',' || character == '"' || character == '\n' || character == '\r')) {
      "\"" + safe.replace("\"", "\"\"") + "\""
    } else safe
  }
}
