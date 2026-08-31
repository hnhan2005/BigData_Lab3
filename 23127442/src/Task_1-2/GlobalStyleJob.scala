package lab3.task12

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Base64

import lab3.common.{Normalization, SaleRowParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.mutable

class GlobalStyleMapper extends Mapper[LongWritable, Text, Text, NullWritable] {
  private val outKey = new Text()

  override protected def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, NullWritable]#Context
  ): Unit = {
    val line = value.toString
    if (!(key.get == 0L && SaleRowParser.isHeader(line))) {
      SaleRowParser.parseCsvRecord(line) match {
        case Right(row)
            if Normalization.isBought(row.status, row.qty) &&
              row.size.exists(Normalization.isAtLeastXXL) =>
          row.style.flatMap(Normalization.normalizeDimension).foreach { style =>
            outKey.set(style)
            context.write(outKey, NullWritable.get())
          }
        case _ =>
      }
    }
  }
}

class GlobalStyleReducer extends Reducer[Text, NullWritable, Text, NullWritable] {
  override protected def reduce(
    key: Text,
    values: java.lang.Iterable[NullWritable],
    context: Reducer[Text, NullWritable, Text, NullWritable]#Context
  ): Unit = context.write(key, NullWritable.get())
}

object GlobalStyleJob {
  private val ConfigurationKey = "lab3.task12.global.qualifying.styles"

  def configure(configuration: Configuration, input: Path, output: Path, reducers: Int): Job = {
    val job = Job.getInstance(configuration, "lab3-task12-global-qualifying-styles")
    job.setJarByClass(classOf[GlobalStyleMapper])
    job.setMapperClass(classOf[GlobalStyleMapper])
    job.setReducerClass(classOf[GlobalStyleReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NullWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    job.setNumReduceTasks(reducers)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  def putStyles(configuration: Configuration, styles: Set[String]): Unit =
    configuration.set(
      ConfigurationKey,
      styles.toSeq.sorted.map(style => Base64.getEncoder.encodeToString(style.getBytes(StandardCharsets.UTF_8))).mkString(",")
    )

  def stylesFrom(configuration: Configuration): Set[String] =
    Option(configuration.get(ConfigurationKey))
      .filter(_.nonEmpty)
      .map(_.split(",", -1).map(encoded => new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)).toSet)
      .getOrElse(Set.empty)

  def readStyles(configuration: Configuration, output: Path): Set[String] = {
    val fileSystem = output.getFileSystem(configuration)
    val styles = mutable.HashSet.empty[String]
    fileSystem.listStatus(output)
      .filter(status => status.isFile && status.getPath.getName.startsWith("part-"))
      .foreach { status =>
        val reader = new BufferedReader(
          new InputStreamReader(fileSystem.open(status.getPath), StandardCharsets.UTF_8)
        )
        try {
          var line = reader.readLine()
          while (line != null) {
            val style = line.trim
            if (style.nonEmpty) styles += style
            line = reader.readLine()
          }
        } finally reader.close()
      }
    styles.toSet
  }
}
