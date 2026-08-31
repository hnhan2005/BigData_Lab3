package lab3.task12

import java.nio.file.Paths

import lab3.common.Cli
import lab3.io.SingleFileExporter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object Task12Driver {
  val Header = "state,month,median_variety,qualifying_style_count"

  def run(
    configuration: Configuration,
    input: Path,
    work: Path,
    outputLocal: java.nio.file.Path,
    reducers: Int,
    overwrite: Boolean
  ): Int = {
    requireSafeWorkPath(work)
    val fileSystem = work.getFileSystem(configuration)
    if (fileSystem.exists(work)) {
      if (!overwrite) throw new IllegalArgumentException("Work path đã tồn tại: " + work)
      if (!fileSystem.delete(work, true)) throw new IllegalStateException("Không xóa được work path: " + work)
    }

    val globalStylePath = new Path(work, "global-qualifying-styles")
    val varietyPath = new Path(work, "style-variety")
    val medianPath = new Path(work, "state-month-median")

    val globalStyleJob = GlobalStyleJob.configure(configuration, input, globalStylePath, reducers)
    if (!globalStyleJob.waitForCompletion(true)) return 1

    val qualifyingStyles = GlobalStyleJob.readStyles(configuration, globalStylePath)
    if (qualifyingStyles.isEmpty) {
      throw new IllegalStateException("Không tìm thấy style có size từ XXL trở lên trên toàn bộ dữ liệu")
    }

    val varietyJob = VarietyJob.configure(configuration, input, varietyPath, reducers, qualifyingStyles)
    if (!varietyJob.waitForCompletion(true)) return 1

    val medianJob = MedianJob.configure(configuration, varietyPath, medianPath)
    if (!medianJob.waitForCompletion(true)) return 1

    SingleFileExporter.exportCsv(fileSystem, medianPath, Header, outputLocal, overwrite)
    0
  }

  private def requireSafeWorkPath(path: Path): Unit = {
    val normalized = Option(path.toUri.getPath).getOrElse("").replace('\\', '/').stripSuffix("/")
    if (normalized.split("/").count(_.nonEmpty) < 2) {
      throw new IllegalArgumentException("Work path quá rộng/không an toàn: " + path)
    }
  }
}

object Task12Main {
  def main(args: Array[String]): Unit = {
    val exitCode = runArgs(args)
    if (exitCode != 0) sys.exit(exitCode)
  }

  def runArgs(args: Array[String]): Int = {
    Cli.parse(args) match {
      case Left(error) => fail(error)
      case Right(options) =>
        Cli.requireValues(options, Seq("input", "work", "output-local")) match {
          case Left(error) => fail(error)
          case Right(_) =>
            Cli.positiveInt(options, "reducers", 2) match {
              case Left(error) => fail(error)
              case Right(reducers) =>
                try {
                  Task12Driver.run(
                    new Configuration(),
                    new Path(options.value("input").get),
                    new Path(options.value("work").get),
                    Paths.get(options.value("output-local").get),
                    reducers,
                    options.flag("overwrite")
                  )
                } catch {
                  case error: Exception => fail(error.getMessage)
                }
            }
        }
    }
  }

  private def fail(message: String): Int = {
    System.err.println("[ERROR] " + message)
    System.err.println(
      "Usage: Task12Main --input <hdfs-path> --work <hdfs-path> --output-local <Task_1-2.csv> [--reducers N] [--overwrite]"
    )
    2
  }
}
