package lab3.task11

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import lab3.common.Cli
import lab3.io.SingleFileExporter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Task11Driver {
  val Header = "state,window_date,window_days,winning_size,frequency,population_variance"

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

    val countsPath = new Path(work, "state-counts")
    val bucketsPath = new Path(work, "buckets")
    val winnersPath = new Path(work, "winners")

    val countJob = BoughtCountJob.configure(configuration, input, countsPath, reducers)
    if (!countJob.waitForCompletion(true)) return 1

    val stateWindows = readStateWindows(fileSystem, countsPath)
    if (stateWindows.isEmpty) throw new IllegalStateException("Không có bought row hợp lệ để tạo cửa sổ")

    val bucketJob = BucketJob.configure(configuration, input, bucketsPath, reducers, stateWindows)
    if (!bucketJob.waitForCompletion(true)) return 1

    val winnerJob = WinnerJob.configure(configuration, bucketsPath, winnersPath, stateWindows)
    if (!winnerJob.waitForCompletion(true)) return 1

    SingleFileExporter.exportCsv(fileSystem, winnersPath, Header, outputLocal, overwrite)
    0
  }

  private[task11] def readStateWindows(fileSystem: FileSystem, countsPath: Path): Map[String, Int] = {
    val parts = fileSystem
      .listStatus(countsPath)
      .filter(status => status.isFile && status.getPath.getName.startsWith("part-"))
      .sortBy(_.getPath.getName)

    parts.flatMap { status =>
      val reader = new BufferedReader(new InputStreamReader(fileSystem.open(status.getPath), StandardCharsets.UTF_8))
      try {
        Iterator.continually(reader.readLine()).takeWhile(_ != null).map { line =>
          val fields = line.split("\t", 2)
          if (fields.length != 2) throw new IllegalStateException("State-count output không hợp lệ: " + line)
          fields(0) -> WindowLength.days(fields(1).toLong)
        }.toVector
      } finally reader.close()
    }.toMap
  }

  private def requireSafeWorkPath(path: Path): Unit = {
    val normalized = Option(path.toUri.getPath).getOrElse("").replace('\\', '/').stripSuffix("/")
    val segments = normalized.split("/").filter(_.nonEmpty)
    if (segments.length < 2) throw new IllegalArgumentException("Work path quá rộng/không an toàn: " + path)
  }
}

object Task11Main {
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
                  Task11Driver.run(
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
      "Usage: Task11Main --input <hdfs-path> --work <hdfs-path> --output-local <Task_1-1.csv> [--reducers N] [--overwrite]"
    )
    2
  }
}
