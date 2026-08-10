package lab3.common

sealed trait DataError {
  def field: String
  def message: String
}

final case class StructuralDataError(message: String) extends DataError {
  override val field: String = "csv"
}

final case class MissingFieldError(field: String) extends DataError {
  override val message: String = "Thiếu giá trị bắt buộc cho cột " + field
}

final case class InvalidFieldError(field: String, rawValue: String, reason: String) extends DataError {
  override val message: String =
    "Giá trị không hợp lệ cho cột " + field + ": '" + rawValue + "' (" + reason + ")"
}
