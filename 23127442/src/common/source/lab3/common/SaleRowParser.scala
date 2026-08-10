package lab3.common

import java.io.StringReader
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.Locale

import org.apache.commons.csv.{CSVFormat, CSVRecord}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SaleRowParser {
  val Header: Vector[String] = Vector(
    "index",
    "Order ID",
    "Date",
    "Status",
    "Fulfilment",
    "Sales Channel ",
    "ship-service-level",
    "Style",
    "SKU",
    "Category",
    "Size",
    "ASIN",
    "Courier Status",
    "Qty",
    "currency",
    "Amount",
    "ship-city",
    "ship-state",
    "ship-postal-code",
    "ship-country",
    "promotion-ids",
    "B2B",
    "fulfilled-by",
    "Unnamed: 22"
  )

  private val DateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseSensitive()
    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
    .appendLiteral('-')
    .appendValue(ChronoField.DAY_OF_MONTH, 2)
    .appendLiteral('-')
    .appendValueReduced(ChronoField.YEAR, 2, 2, 2000)
    .toFormatter(Locale.ROOT)
    .withResolverStyle(ResolverStyle.STRICT)

  def isHeader(line: String): Boolean =
    line != null && line.stripPrefix("\uFEFF").startsWith("index,Order ID,Date,")

  def validateHeader(line: String): Either[DataError, Unit] = {
    parseRecord(line.stripPrefix("\uFEFF")) match {
      case Left(error) => Left(error)
      case Right(record) =>
        val values = record.iterator().asScala.toVector
        if (values == Header) Right(())
        else Left(StructuralDataError("CSV header không khớp schema 24 cột bắt buộc"))
    }
  }

  def parseCsvRecord(line: String): Either[DataError, SaleRow] = {
    parseRecord(line) match {
      case Left(error) => Left(error)
      case Right(record) if record.size() != Header.size =>
        Left(StructuralDataError("CSV record có " + record.size() + " cột; cần " + Header.size))
      case Right(record) => parseSaleRow(record)
    }
  }

  private def parseRecord(line: String): Either[DataError, CSVRecord] = {
    if (line == null) return Left(StructuralDataError("CSV record là null"))
    try {
      val records = CSVFormat.DEFAULT.parse(new StringReader(line)).getRecords.asScala
      if (records.size != 1) Left(StructuralDataError("Mỗi input line phải chứa đúng một CSV record"))
      else Right(records.head)
    } catch {
      case NonFatal(error) => Left(StructuralDataError("Không parse được CSV: " + error.getMessage))
    }
  }

  private def parseSaleRow(record: CSVRecord): Either[DataError, SaleRow] = {
    for {
      recordId <- requiredLong(record.get(0), "index").right
      orderId <- requiredText(record.get(1), "Order ID").right
      date <- requiredDate(record.get(2), "Date").right
      status <- requiredText(record.get(3), "Status").right
      fulfilment <- requiredText(record.get(4), "Fulfilment").right
      serviceLevel <- requiredText(record.get(6), "ship-service-level").right
      qty <- requiredLong(record.get(13), "Qty").right
      amount <- optionalDouble(record.get(15), "Amount").right
    } yield SaleRow(
      recordId = recordId,
      orderId = orderId,
      date = date,
      status = status,
      fulfilment = fulfilment,
      serviceLevel = serviceLevel,
      style = optionalText(record.get(7)),
      sku = optionalText(record.get(8)),
      size = optionalText(record.get(10)),
      courierStatus = optionalText(record.get(12)),
      qty = qty,
      amount = amount,
      city = optionalText(record.get(16)),
      state = optionalText(record.get(17)),
      promotionIds = Normalization.parsePromotions(record.get(20))
    )
  }

  private def requiredText(raw: String, field: String): Either[DataError, String] =
    optionalText(raw) match {
      case Some(value) => Right(value)
      case None => Left(MissingFieldError(field))
    }

  private def optionalText(raw: String): Option[String] =
    Option(raw).map(_.trim).filter(_.nonEmpty)

  private def requiredLong(raw: String, field: String): Either[DataError, Long] =
    requiredText(raw, field) match {
      case Left(error) => Left(error)
      case Right(value) =>
        try Right(value.toLong)
        catch {
          case _: NumberFormatException => Left(InvalidFieldError(field, value, "cần số nguyên"))
        }
    }

  private def optionalDouble(raw: String, field: String): Either[DataError, Option[Double]] =
    optionalText(raw) match {
      case None => Right(None)
      case Some(value) =>
        try {
          val parsed = value.toDouble
          if (parsed.isNaN || parsed.isInfinity) Left(InvalidFieldError(field, value, "cần số hữu hạn"))
          else Right(Some(parsed))
        } catch {
          case _: NumberFormatException => Left(InvalidFieldError(field, value, "cần số thực"))
        }
    }

  private def requiredDate(raw: String, field: String): Either[DataError, LocalDate] =
    requiredText(raw, field) match {
      case Left(error) => Left(error)
      case Right(value) =>
        try Right(LocalDate.parse(value, DateFormatter))
        catch {
          case NonFatal(_) => Left(InvalidFieldError(field, value, "cần định dạng MM-dd-yy và ngày hợp lệ"))
        }
    }
}
