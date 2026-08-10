package lab3.task11

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class Task11LocalIntegrationSpec extends FlatSpec with Matchers {
  "Task11Driver" should "run all three Hadoop jobs and export one deterministic CSV" in {
    if (System.getProperty("os.name", "").toLowerCase.contains("windows")) {
      cancel("Hadoop 3.3.6 local mode requires its native Windows layer; this integration test runs on Linux/Lab 1")
    }

    val root = Files.createTempDirectory("lab3-task11-it-")
    val input = root.resolve("shared-sales.csv")
    val output = root.resolve("Task_1-1.csv")
    val resource = Paths.get(getClass.getResource("/fixtures/shared-sales.csv").toURI)
    Files.copy(resource, input)

    val configuration = new Configuration()
    configuration.set("mapreduce.framework.name", "local")
    configuration.set("fs.defaultFS", "file:///")
    configuration.set("hadoop.tmp.dir", root.resolve("hadoop-tmp").toString)

    val exit = Task11Driver.run(
      configuration,
      new Path(input.toUri),
      new Path(root.resolve("work").toUri),
      output,
      reducers = 1,
      overwrite = false
    )

    exit shouldBe 0
    Files.isRegularFile(output) shouldBe true
    val lines = Files.readAllLines(output, StandardCharsets.UTF_8).asScala.toVector
    lines.head shouldBe Task11Driver.Header
    lines.tail.size shouldBe 20
    lines.tail.count(_.startsWith("STATE A,")) shouldBe 10
    lines.tail.count(_.startsWith("STATE B,")) shouldBe 10
    lines should contain("STATE A,2022-04-02,10,XXL,1,0.0")
    lines should contain("STATE B,2022-04-13,10,3XL,1,")
    lines.tail.map(_.split(",").take(2).mkString(",")).distinct.size shouldBe 20
  }
}
