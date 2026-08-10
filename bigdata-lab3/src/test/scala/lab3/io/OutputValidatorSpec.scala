package lab3.io

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}

class OutputValidatorSpec extends FlatSpec with Matchers {
  "OutputValidator" should "accept valid Task 1 CSV contracts" in {
    val root = Files.createTempDirectory("lab3-output-validator-")
    val task11 = root.resolve("Task_1-1.csv")
    Files.write(task11, (
      Task11Header + "\nSTATE,2022-04-02,10,XXL,2,25.0\nSTATE,2022-04-03,10,L,1,\n"
    ).getBytes(StandardCharsets.UTF_8))
    OutputValidator.validateTask11Csv(task11)

    val task12 = root.resolve("Task_1-2.csv")
    Files.write(task12, (Task12Header + "\nSTATE,2022-04,2.5,4\n").getBytes(StandardCharsets.UTF_8))
    OutputValidator.validateTask12Csv(task12)
  }

  it should "reject duplicate logical keys and invalid values" in {
    val path = Files.createTempFile("lab3-invalid-task12-", ".csv")
    Files.write(path, (
      Task12Header + "\nSTATE,2022-04,1.0,1\nSTATE,2022-04,0.0,0\n"
    ).getBytes(StandardCharsets.UTF_8))
    an[IllegalArgumentException] should be thrownBy OutputValidator.validateTask12Csv(path)
  }

  private val Task11Header = OutputValidator.Task11Header.mkString(",")
  private val Task12Header = OutputValidator.Task12Header.mkString(",")
}
