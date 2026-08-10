package lab3.task11

import lab3.common.{Normalization, SaleRowParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class BucketMapper extends Mapper[LongWritable, Text, Text, MomentWritable] {
  private var stateWindows = Map.empty[String, Int]
  private val outKey = new Text()
  private val outValue = new MomentWritable()

  override protected def setup(
    context: Mapper[LongWritable, Text, Text, MomentWritable]#Context
  ): Unit = {
    stateWindows = WindowConfig.decode(context.getConfiguration.get(WindowConfig.Key, ""))
  }

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, MomentWritable]#Context
  ): Unit = {
    val line = value.toString
    if (key.get == 0L && SaleRowParser.isHeader(line)) return

    SaleRowParser.parseCsvRecord(line) match {
      case Left(error) =>
        context.getCounter("LAB3_REJECTED", error.field.toUpperCase.replace(' ', '_')).increment(1L)
      case Right(row) if !Normalization.isBought(row.status, row.qty) => ()
      case Right(row) =>
        val dimensions = for {
          stateRaw <- row.state
          state <- Normalization.normalizeDimension(stateRaw)
          sizeRaw <- row.size
          size <- Normalization.normalizeDimension(sizeRaw)
          days <- stateWindows.get(state)
        } yield (state, size, days)

        dimensions match {
          case None => context.getCounter("LAB3_REJECTED", "MISSING_BUCKET_DIMENSION").increment(1L)
          case Some((state, size, days)) =>
            outValue.set(Moment.fromAmount(row.amount))
            WindowBuckets.dates(row.date, days).foreach { bucketDate =>
              outKey.set(Task11Keys.bucket(state, bucketDate, size))
              context.write(outKey, outValue)
              context.getCounter("LAB3_TASK11", "BUCKETS_EMITTED").increment(1L)
            }
        }
    }
  }
}

class MomentCombiner extends Reducer[Text, MomentWritable, Text, MomentWritable] {
  private val outValue = new MomentWritable()

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[MomentWritable],
    context: Reducer[Text, MomentWritable, Text, MomentWritable]#Context
  ): Unit = {
    val iterator = values.iterator()
    var aggregate = Moment.Empty
    while (iterator.hasNext) aggregate = aggregate.combine(iterator.next().toMoment)
    outValue.set(aggregate)
    context.write(key, outValue)
  }
}

class BucketReducer extends Reducer[Text, MomentWritable, Text, Text] {
  private val outKey = new Text()
  private val outValue = new Text()

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[MomentWritable],
    context: Reducer[Text, MomentWritable, Text, Text]#Context
  ): Unit = {
    val iterator = values.iterator()
    var aggregate = Moment.Empty
    while (iterator.hasNext) aggregate = aggregate.combine(iterator.next().toMoment)
    val (state, date, size) = Task11Keys.parseBucket(key.toString)
    outKey.set(Task11Keys.window(state, date))
    outValue.set(Task11Keys.candidate(SizeCandidate(size, aggregate)))
    context.write(outKey, outValue)
  }
}

object BucketJob {
  def configure(
    configuration: Configuration,
    input: Path,
    output: Path,
    reducers: Int,
    stateWindows: Map[String, Int]
  ): Job = {
    val jobConfiguration = new Configuration(configuration)
    jobConfiguration.set(WindowConfig.Key, WindowConfig.encode(stateWindows))
    val job = Job.getInstance(jobConfiguration, "lab3-task11-window-buckets")
    job.setJarByClass(classOf[BucketMapper])
    job.setMapperClass(classOf[BucketMapper])
    job.setCombinerClass(classOf[MomentCombiner])
    job.setReducerClass(classOf[BucketReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[MomentWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(reducers)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }
}
