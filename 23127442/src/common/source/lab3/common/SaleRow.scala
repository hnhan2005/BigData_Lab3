package lab3.common

import java.time.LocalDate

final case class SaleRow(
  recordId: Long,
  orderId: String,
  date: LocalDate,
  status: String,
  fulfilment: String,
  serviceLevel: String,
  style: Option[String],
  sku: Option[String],
  size: Option[String],
  courierStatus: Option[String],
  qty: Long,
  amount: Option[Double],
  city: Option[String],
  state: Option[String],
  promotionIds: Vector[String]
) {
  def month: String = f"${date.getYear}%04d-${date.getMonthValue}%02d"
}
