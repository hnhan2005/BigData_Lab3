package lab3.io

import java.io.{BufferedReader, BufferedWriter, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path => NioPath, StandardCopyOption}
import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object SingleFileExporter {
  def exportParquet(dataFrame: DataFrame, localTarget: NioPath, overwrite: Boolean): Unit = {
    if (Files.exists(localTarget) && !overwrite) {
      throw new IllegalArgumentException("Output đã tồn tại: " + localTarget)
    }
    val parent = Option(localTarget.toAbsolutePath.getParent)
      .getOrElse(throw new IllegalArgumentException("Output local không có thư mục cha"))
    Files.createDirectories(parent)
    val temporaryDirectory = parent.resolve(localTarget.getFileName.toString + ".dir-" + UUID.randomUUID().toString)
    val temporaryFile = parent.resolve(localTarget.getFileName.toString + ".tmp-" + UUID.randomUUID().toString)

    try {
      dataFrame.coalesce(1).write.mode("error").parquet(temporaryDirectory.toString)
      val writtenSchema = dataFrame.sparkSession.read.parquet(temporaryDirectory.toString).schema
      val writtenContract = writtenSchema.fields.map(field => field.name -> field.dataType.catalogString).toVector
      val expectedContract = dataFrame.schema.fields.map(field => field.name -> field.dataType.catalogString).toVector
      if (writtenContract != expectedContract) {
        throw new IllegalStateException("Schema Parquet read-back không khớp DataFrame output")
      }
      val stream = Files.list(temporaryDirectory)
      val parts = try stream.iterator().asScala
        .filter(path => path.getFileName.toString.startsWith("part-") && path.getFileName.toString.endsWith(".parquet"))
        .toVector
      finally stream.close()
      if (parts.size != 1) throw new IllegalStateException("Cần đúng một part Parquet, nhận được " + parts.size)
      Files.move(parts.head, temporaryFile, StandardCopyOption.REPLACE_EXISTING)
      moveIntoPlace(temporaryFile, localTarget, overwrite)
    } catch {
      case error: Throwable =>
        Files.deleteIfExists(temporaryFile)
        throw error
    } finally deleteTree(temporaryDirectory)
  }

  def exportCsv(
    fileSystem: FileSystem,
    hadoopOutput: Path,
    header: String,
    localTarget: NioPath,
    overwrite: Boolean
  ): Unit = {
    if (Files.exists(localTarget) && !overwrite) {
      throw new IllegalArgumentException("Output đã tồn tại: " + localTarget)
    }

    val parts = fileSystem
      .listStatus(hadoopOutput)
      .filter(status => status.isFile && status.getPath.getName.startsWith("part-"))
      .sortBy(_.getPath.getName)

    if (parts.isEmpty) throw new IllegalStateException("Không tìm thấy part file trong " + hadoopOutput)

    val parent = Option(localTarget.toAbsolutePath.getParent)
      .getOrElse(throw new IllegalArgumentException("Output local không có thư mục cha"))
    Files.createDirectories(parent)
    val temporary = parent.resolve(localTarget.getFileName.toString + ".tmp-" + UUID.randomUUID().toString)

    try {
      val writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)
      try {
        writer.write(header)
        writer.newLine()
        parts.foreach(status => appendPart(fileSystem, status.getPath, writer))
      } finally writer.close()

      moveIntoPlace(temporary, localTarget, overwrite)
    } catch {
      case error: Throwable =>
        Files.deleteIfExists(temporary)
        throw error
    }
  }

  private def appendPart(fileSystem: FileSystem, part: Path, writer: BufferedWriter): Unit = {
    val reader = new BufferedReader(new InputStreamReader(fileSystem.open(part), StandardCharsets.UTF_8))
    try {
      var line = reader.readLine()
      while (line != null) {
        writer.write(line)
        writer.newLine()
        line = reader.readLine()
      }
    } finally reader.close()
  }

  private def moveIntoPlace(temporary: NioPath, target: NioPath, overwrite: Boolean): Unit = {
    val options =
      if (overwrite) Array(StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      else Array(StandardCopyOption.ATOMIC_MOVE)
    try Files.move(temporary, target, options: _*)
    catch {
      case _: AtomicMoveNotSupportedException =>
        if (overwrite) Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
        else Files.move(temporary, target)
    }
  }

  private def deleteTree(root: NioPath): Unit = {
    if (Files.exists(root)) {
      val stream = Files.walk(root)
      try stream.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally stream.close()
    }
  }
}
