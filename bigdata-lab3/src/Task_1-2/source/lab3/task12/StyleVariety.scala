package lab3.task12

import scala.collection.mutable

object StyleVariety {
  def qualifyingVariety(entries: Iterator[(String, Boolean)]): Option[Long] = {
    val distinctSkus = mutable.HashSet.empty[String]
    var hasAtLeastXXL = false
    entries.foreach {
      case (sku, qualifies) =>
        distinctSkus += sku
        hasAtLeastXXL = hasAtLeastXXL || qualifies
    }
    if (hasAtLeastXXL) Some(distinctSkus.size.toLong) else None
  }
}
