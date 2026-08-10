package lab3.task11

import lab3.common.CsvEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class CandidateMapper extends Mapper[LongWritable, Text, Text, Text] {
  private val outKey = new Text()
  private val outValue = new Text()

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, Text]#Context
  ): Unit = {
    val line = value.toString
    val separator = line.indexOf('\t')
    if (separator <= 0 || separator == line.length - 1) {
      context.getCounter("LAB3_REJECTED", "INVALID_CANDIDATE_LINE").increment(1L)
    } else {
      outKey.set(line.substring(0, separator))
      outValue.set(line.substring(separator + 1))
      context.write(outKey, outValue)
    }
  }
}

class WinnerReducer extends Reducer[Text, Text, NullWritable, Text] {
  private var stateWindows = Map.empty[String, Int]
  private val outValue = new Text()

  override protected def setup(
    context: Reducer[Text, Text, NullWritable, Text]#Context
  ): Unit = {
    stateWindows = WindowConfig.decode(context.getConfiguration.get(WindowConfig.Key, ""))
  }

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[Text],
    context: Reducer[Text, Text, NullWritable, Text]#Context
  ): Unit = {
    val iterator = values.iterator()
    var winner: Option[SizeCandidate] = None
    while (iterator.hasNext) {
      val candidate = Task11Keys.parseCandidate(iterator.next().toString)
      winner = winner match {
        case None => Some(candidate)
        case Some(current) => Some(Winner.better(current, candidate))
      }
    }

    winner.foreach { candidate =>
      val (state, date) = Task11Keys.parseWindow(key.toString)
      val variance = candidate.moment.populationVariance.map(_.toString).getOrElse("")
      outValue.set(
        CsvEncoding.row(
          Seq(state, date, stateWindows(state), candidate.size, candidate.moment.count, variance)
        )
      )
      context.write(NullWritable.get(), outValue)
    }
  }
}

object WinnerJob {
  def configure(
    configuration: Configuration,
    input: Path,
    output: Path,
    stateWindows: Map[String, Int]
  ): Job = {
    val jobConfiguration = new Configuration(configuration)
    jobConfiguration.set(WindowConfig.Key, WindowConfig.encode(stateWindows))
    val job = Job.getInstance(jobConfiguration, "lab3-task11-window-winner")
    job.setJarByClass(classOf[CandidateMapper])
    job.setMapperClass(classOf[CandidateMapper])
    job.setReducerClass(classOf[WinnerReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }
}
