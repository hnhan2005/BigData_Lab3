package lab3.task12

object Median {
  def exact(values: IndexedSeq[Long]): Either[String, Double] = {
    if (values.isEmpty) Left("Không thể tính median cho tập rỗng")
    else {
      val sorted = values.sorted
      val middle = sorted.length / 2
      if (sorted.length % 2 == 1) Right(sorted(middle).toDouble)
      else Right(sorted(middle - 1).toDouble / 2.0 + sorted(middle).toDouble / 2.0)
    }
  }
}
