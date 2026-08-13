package lab3.task12

object Task12Keys {
  private val Separator = "\u0001"

  def style(state: String, month: String, style: String): String =
    Seq(state, month, style).mkString(Separator)

  def parseStyle(raw: String): (String, String, String) = {
    val parts = raw.split(Separator, -1)
    require(parts.length == 3, "Invalid style key")
    (parts(0), parts(1), parts(2))
  }

  def stateMonth(state: String, month: String): String = Seq(state, month).mkString(Separator)

  def parseStateMonth(raw: String): (String, String) = {
    val parts = raw.split(Separator, -1)
    require(parts.length == 2, "Invalid state-month key")
    (parts(0), parts(1))
  }

  def skuAndSize(sku: String, atLeastXXL: Boolean): String =
    sku + Separator + (if (atLeastXXL) "1" else "0")

  def parseSkuAndSize(raw: String): (String, Boolean) = {
    val parts = raw.split(Separator, -1)
    require(parts.length == 2, "Invalid SKU/size value")
    (parts(0), parts(1) == "1")
  }
}
