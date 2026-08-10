package lab3.task12

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class Task12LocalIntegrationSpec extends FlatSpec with Matchers {
  "Task12Driver" should "run both Hadoop jobs and export exact median-variety rows" in {
    if (System.getProperty("os.name", "").toLowerCase.contains("windows")) {
      cancel("Hadoop 3.3.6 local mode requires its native Windows layer; this integration test runs on Linux/Lab 1")
    }

    val root = Files.createTempDirectory("lab3-task12-it-")
    val input = root.resolve("shared-sales.csv")
    val output = root.resolve("Task_1-2.csv")
    Files.copy(Paths.get(getClass.getResource("/fixtures/shared-sales.csv").toURI), input)

    val configuration = new Configuration()
    configuration.set("mapreduce.framework.name", "local")
    configuration.set("fs.defaultFS", "file:///")
    configuration.set("hadoop.tmp.dir", root.resolve("hadoop-tmp").toString)

    Task12Driver.run(
      configuration,
      new Path(input.toUri),
      new Path(root.resolve("work").toUri),
      output,
      reducers = 1,
      overwrite = false
    ) shouldBe 0

    Files.readAllLines(output, StandardCharsets.UTF_8).asScala.toVector shouldBe Vector(
      Task12Driver.Header,
      "STATE A,2022-04,1.0,1",
      "STATE B,2022-04,1.0,1"
    )
  }
}
