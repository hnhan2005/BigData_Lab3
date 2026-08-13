package lab3.task12

import lab3.common.CsvEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.mutable.ArrayBuffer

class VarietyOutputMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
  private val outKey = new Text()
  private val outValue = new LongWritable()

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = {
    val parts = value.toString.split("\t", 2)
    if (parts.length != 2) {
      context.getCounter("LAB3_REJECTED", "INVALID_VARIETY_INTERMEDIATE").increment(1L)
    } else {
      try {
        Task12Keys.parseStateMonth(parts(0))
        val variety = parts(1).toLong
        require(variety > 0L, "variety must be positive")
        outKey.set(parts(0))
        outValue.set(variety)
        context.write(outKey, outValue)
      } catch {
        case _: IllegalArgumentException =>
          context.getCounter("LAB3_REJECTED", "INVALID_VARIETY_INTERMEDIATE").increment(1L)
      }
    }
  }
}

class MedianReducer extends Reducer[Text, LongWritable, Text, NullWritable] {
  private val output = new Text()

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[LongWritable],
    context: Reducer[Text, LongWritable, Text, NullWritable]#Context
  ): Unit = {
    val varieties = ArrayBuffer.empty[Long]
    val iterator = values.iterator()
    while (iterator.hasNext) varieties += iterator.next().get

    Median.exact(varieties) match {
      case Left(_) => context.getCounter("LAB3_REJECTED", "EMPTY_MEDIAN_GROUP").increment(1L)
      case Right(median) =>
        val (state, month) = Task12Keys.parseStateMonth(key.toString)
        output.set(CsvEncoding.row(Seq(state, month, median.toString, varieties.size.toString)))
        context.write(output, NullWritable.get())
        context.getCounter("LAB3_DATA", "STATE_MONTH_GROUPS").increment(1L)
    }
  }
}

object MedianJob {
  def configure(configuration: Configuration, input: Path, output: Path): Job = {
    val job = Job.getInstance(configuration, "lab3-task12-state-month-median")
    job.setJarByClass(classOf[VarietyOutputMapper])
    job.setMapperClass(classOf[VarietyOutputMapper])
    job.setReducerClass(classOf[MedianReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }
}
