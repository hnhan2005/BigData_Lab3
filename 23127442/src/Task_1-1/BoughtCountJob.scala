package lab3.task11

import lab3.common.{Normalization, SaleRowParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class BoughtCountMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
  private val outKey = new Text()
  private val one = new LongWritable(1L)

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = {
    val line = value.toString
    context.getCounter("LAB3_DATA", "ROWS_READ").increment(1L)

    if (key.get == 0L && SaleRowParser.isHeader(line)) {
      context.getCounter("LAB3_DATA", "HEADER_ROWS").increment(1L)
      return
    }

    SaleRowParser.parseCsvRecord(line) match {
      case Left(error) =>
        context.getCounter("LAB3_REJECTED", error.field.toUpperCase.replace(' ', '_')).increment(1L)
      case Right(row) if !Normalization.isBought(row.status, row.qty) =>
        context.getCounter("LAB3_DATA", "NOT_BOUGHT").increment(1L)
      case Right(row) =>
        row.state.flatMap(Normalization.normalizeDimension) match {
          case None => context.getCounter("LAB3_REJECTED", "MISSING_STATE").increment(1L)
          case Some(state) =>
            context.getCounter("LAB3_DATA", "BOUGHT_ROWS").increment(1L)
            outKey.set(state)
            context.write(outKey, one)
        }
    }
  }
}

class LongSumReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
  private val outValue = new LongWritable()

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[LongWritable],
    context: Reducer[Text, LongWritable, Text, LongWritable]#Context
  ): Unit = {
    val iterator = values.iterator()
    var sum = 0L
    while (iterator.hasNext) sum += iterator.next().get
    outValue.set(sum)
    context.write(key, outValue)
  }
}

object BoughtCountJob {
  def configure(
    configuration: Configuration,
    input: Path,
    output: Path,
    reducers: Int
  ): Job = {
    val job = Job.getInstance(configuration, "lab3-task11-state-bought-count")
    job.setJarByClass(classOf[BoughtCountMapper])
    job.setMapperClass(classOf[BoughtCountMapper])
    job.setCombinerClass(classOf[LongSumReducer])
    job.setReducerClass(classOf[LongSumReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    job.setNumReduceTasks(reducers)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }
}
