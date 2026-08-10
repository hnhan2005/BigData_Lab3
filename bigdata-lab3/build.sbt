import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / organization := "vn.edu.hcmus"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "bigdata-lab3",
    Compile / unmanagedSourceDirectories ++= Seq(
      baseDirectory.value / "src" / "common" / "source",
      baseDirectory.value / "src" / "Task_1-1" / "source",
      baseDirectory.value / "src" / "Task_1-2" / "source",
      baseDirectory.value / "src" / "Task_2-1" / "source",
      baseDirectory.value / "src" / "Task_2-2" / "source"
    ),
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "scala",
    Test / unmanagedResourceDirectories += baseDirectory.value / "src" / "test" / "resources",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % "3.3.6" % "provided,test",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6" % "provided,test",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6" % "provided,test",
      ("org.apache.spark" %% "spark-sql" % "2.4.8" % "provided,test")
        .excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
      "org.apache.commons" % "commons-csv" % "1.10.0",
      "org.scalatest" %% "scalatest" % "3.0.8" % Test
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3"
    ),
    Test / parallelExecution := false,
    Test / fork := true,
    javaOptions ++= Seq("-Duser.timezone=UTC", "-Dfile.encoding=UTF-8"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "UTF-8"),
    assembly / assemblyJarName := "bigdata-lab3.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)              => MergeStrategy.discard
      case "reference.conf"                          => MergeStrategy.concat
      case "application.conf"                        => MergeStrategy.concat
      case _                                         => MergeStrategy.first
    }
  )
