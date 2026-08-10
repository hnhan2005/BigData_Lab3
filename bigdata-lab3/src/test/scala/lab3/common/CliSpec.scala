package lab3.common

import org.scalatest.{FlatSpec, Matchers}

class CliSpec extends FlatSpec with Matchers {
  "Cli.parse" should "parse values and flags" in {
    Cli.parse(Array("--input", "hdfs:///lab3/asr.csv", "--runs", "5", "--overwrite")) shouldBe
      Right(
        ParsedOptions(
          Map("input" -> "hdfs:///lab3/asr.csv", "runs" -> "5"),
          Set("overwrite")
        )
      )
  }

  it should "reject positional and duplicate arguments" in {
    Cli.parse(Array("input.csv")).isLeft shouldBe true
    Cli.parse(Array("--input", "a", "--input", "b")).isLeft shouldBe true
  }

  "Cli.requireValues" should "report every missing required option" in {
    val options = ParsedOptions(Map("input" -> " "), Set.empty)
    Cli.requireValues(options, Seq("input", "output-local")).left.get should include("--input")
    Cli.requireValues(options, Seq("input", "output-local")).left.get should include("--output-local")
  }

  "Cli.positiveInt" should "accept a positive value or use the default" in {
    Cli.positiveInt(ParsedOptions(Map.empty, Set.empty), "runs", 5) shouldBe Right(5)
    Cli.positiveInt(ParsedOptions(Map("runs" -> "7"), Set.empty), "runs", 5) shouldBe Right(7)
  }

  it should "reject zero, negative, and non-numeric values" in {
    Seq("0", "-1", "five").foreach { raw =>
      Cli.positiveInt(ParsedOptions(Map("runs" -> raw), Set.empty), "runs", 5).isLeft shouldBe true
    }
  }

  "Cli.uri" should "allow only configured URI schemes" in {
    val hdfs = ParsedOptions(Map("input" -> "hdfs:///lab3/asr.csv"), Set.empty)
    Cli.uri(hdfs, "input", Set("hdfs", "file")).right.get.getScheme shouldBe "hdfs"

    val http = ParsedOptions(Map("input" -> "https://example.com/asr.csv"), Set.empty)
    Cli.uri(http, "input", Set("hdfs", "file")).isLeft shouldBe true
  }

  "Cli.validateOutputState" should "require explicit overwrite for an existing target" in {
    Cli.validateOutputState(exists = true, overwrite = false).isLeft shouldBe true
    Cli.validateOutputState(exists = true, overwrite = true) shouldBe Right(())
    Cli.validateOutputState(exists = false, overwrite = false) shouldBe Right(())
  }
}
