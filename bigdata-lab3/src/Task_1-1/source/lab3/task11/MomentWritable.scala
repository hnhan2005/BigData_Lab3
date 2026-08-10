package lab3.task11

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

class MomentWritable() extends Writable {
  private var countValue = 0L
  private var amountCountValue = 0L
  private var sumValue = 0.0
  private var sumSquaresValue = 0.0

  def this(moment: Moment) = {
    this()
    set(moment)
  }

  def set(moment: Moment): Unit = {
    countValue = moment.count
    amountCountValue = moment.amountCount
    sumValue = moment.sum
    sumSquaresValue = moment.sumSquares
  }

  def toMoment: Moment = Moment(countValue, amountCountValue, sumValue, sumSquaresValue)

  override def write(out: DataOutput): Unit = {
    out.writeLong(countValue)
    out.writeLong(amountCountValue)
    out.writeDouble(sumValue)
    out.writeDouble(sumSquaresValue)
  }

  override def readFields(in: DataInput): Unit = {
    countValue = in.readLong()
    amountCountValue = in.readLong()
    sumValue = in.readDouble()
    sumSquaresValue = in.readDouble()
  }
}
