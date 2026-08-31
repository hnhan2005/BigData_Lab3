package lab3.task12

import lab3.common.{Normalization, SaleRowParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConverters._

class VarietyMapper extends Mapper[LongWritable, Text, Text, Text] {
  private val outKey = new Text()
  private val outValue = new Text()
  private var qualifyingStyles = Set.empty[String]

  override protected def setup(
    context: Mapper[LongWritable, Text, Text, Text]#Context
  ): Unit = {
    qualifyingStyles = GlobalStyleJob.stylesFrom(context.getConfiguration)
  }

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, Text]#Context
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
        val dimensions = for {
          state <- row.state.flatMap(Normalization.normalizeDimension)
          style <- row.style.flatMap(Normalization.normalizeDimension)
          sku <- row.sku.flatMap(Normalization.normalizeDimension)
        } yield (state, style, sku)

        dimensions match {
          case None => context.getCounter("LAB3_REJECTED", "MISSING_STATE_STYLE_OR_SKU").increment(1L)
          case Some((state, style, sku)) if qualifyingStyles.contains(style) =>
            outKey.set(Task12Keys.style(state, row.month, style))
            outValue.set(sku)
            context.write(outKey, outValue)
            context.getCounter("LAB3_DATA", "BOUGHT_COMPLETE_ROWS").increment(1L)
          case Some(_) =>
            context.getCounter("LAB3_DATA", "NON_QUALIFYING_STYLE_ROWS").increment(1L)
        }
    }
  }
}

class VarietyReducer extends Reducer[Text, Text, Text, LongWritable] {
  private val outKey = new Text()
  private val outValue = new LongWritable()

  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[Text],
    context: Reducer[Text, Text, Text, LongWritable]#Context
  ): Unit = {
    val variety = StyleVariety.distinctVariety(values.iterator().asScala.map(_.toString))
    val (state, month, _) = Task12Keys.parseStyle(key.toString)
    outKey.set(Task12Keys.stateMonth(state, month))
    outValue.set(variety)
    context.write(outKey, outValue)
    context.getCounter("LAB3_DATA", "QUALIFYING_STYLES").increment(1L)
  }
}

object VarietyJob {
  def configure(
    configuration: Configuration,
    input: Path,
    output: Path,
    reducers: Int,
    qualifyingStyles: Set[String]
  ): Job = {
    val jobConfiguration = new Configuration(configuration)
    GlobalStyleJob.putStyles(jobConfiguration, qualifyingStyles)
    val job = Job.getInstance(jobConfiguration, "lab3-task12-style-variety")
    job.setJarByClass(classOf[VarietyMapper])
    job.setMapperClass(classOf[VarietyMapper])
    job.setReducerClass(classOf[VarietyReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    job.setNumReduceTasks(reducers)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }
}
