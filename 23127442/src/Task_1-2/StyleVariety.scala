package lab3.task12

import scala.collection.mutable

object StyleVariety {
  def distinctVariety(skus: Iterator[String]): Long = {
    val distinctSkus = mutable.HashSet.empty[String]
    skus.foreach(distinctSkus += _)
    distinctSkus.size.toLong
  }
}
