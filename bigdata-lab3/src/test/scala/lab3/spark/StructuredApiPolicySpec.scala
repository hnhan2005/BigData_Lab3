package lab3.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class StructuredApiPolicySpec extends FlatSpec with Matchers {
  "Task 2 source" should "not execute direct Spark SQL query strings" in {
    task2Sources.foreach { path =>
      val source = read(path)
      withClue(path.toString) {
        "(?s).*\\bspark\\s*\\.\\s*sql\\s*\\(.*".r.findFirstIn(source) shouldBe empty
      }
    }
  }

  it should "use functions.expr only at the reviewed percentile_approx location" in {
    val usages = task2Sources.flatMap { path =>
      read(path).lines.zipWithIndex.collect {
        case (line, index) if line.contains("expr(") => (path, index + 1, line.trim)
      }
    }
    usages.foreach {
      case (path, _, line) =>
        path.getFileName.toString shouldBe "Task22Pipeline.scala"
        line should include("percentile_approx")
    }
    usages.size should be <= 1
  }

  private def task2Sources: Vector[Path] = {
    val roots = Vector(Paths.get("src/Task_2-1/source"), Paths.get("src/Task_2-2/source"))
    roots.filter(Files.exists(_)).flatMap { root =>
      val stream = Files.walk(root)
      try stream.iterator().asScala.filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala")).toVector
      finally stream.close()
    }
  }

  private def read(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
