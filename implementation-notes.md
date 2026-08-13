# Big Data Lab 3 - Implementation Notes

> Running log for decisions not covered by the spec, design deviations, trade-offs, and surprises. Update during execution; do not batch at the end.

## Decisions

- 2026-08-10 - Code execution started after explicit approval of Goals, Design, and Implementation Checklist.
- 2026-08-10 - No local `.agents/skills/` directory exists, so there is no local Scala/Hadoop/Spark skill to activate. Skill discovery will be repeated at every phase as required by the checklist.
- 2026-08-12 - Working from the `23127442/` tree for the requested layout cleanup and README rewrite.
- 2026-08-13 - Flattened all task source files to direct `src/Task_*` roots; `build.sbt` already matched that layout, so no build-path change was needed.
- 2026-08-13 - Rewrote `23127442/docs/README.md` in accented Vietnamese while preserving the existing install/run/validate/package commands and path contract.
- 2026-08-13 - On this Windows workspace, `java`, `scala`, `sbt`, `hadoop`, and `spark-submit` were not on PATH and WSL was not installed. I bootstrapped a portable Temurin JDK 8 plus an `sbt-launch` jar to run the repo checks.
- 2026-08-13 - Added a lightweight Python comparison pipeline under `23127442/python/` so the lab can emit the four CSV outputs directly from `Amazon Sale Report.csv`.
- 2026-08-13 - `python/main.py` now resolves the workspace-root CSV by default and still accepts `--input` / `--output-dir` overrides for portability.

## Design Deviations

- None at execution start.
- 2026-08-12 - The user only requested flattening the task roots, so `src/common/source` is being left in place for now.

## Trade-offs

- 2026-08-10 - Spark 2.4.8 normally brings Hadoop 2.x transitively. The build excludes those transitive Hadoop modules and pins Hadoop 3.3.6 explicitly to avoid two Hadoop generations on the compile/test classpath. Target Spark runtime verification is still required in Lab 1.
- 2026-08-10 - Hadoop 3.3.6 resolves Jackson 2.12 while Spark 2.4.8's `jackson-module-scala` requires Jackson 2.6. The compile/test classpath overrides Jackson core/annotations/databind to Spark-compatible 2.6.7/2.6.7.3. These runtime libraries remain `provided` transitively and are not bundled; the Lab 1 `spark-submit` distribution remains authoritative and must pass preflight/E2E.
- 2026-08-12 - The tree flattening is limited to task roots (`src/Task_*`) so the shared common source layout does not need a second, unrelated migration.
- 2026-08-13 - Recursive deletion of the old empty task folders is blocked by workspace policy, so hidden `.lab3` / `.source` leftovers remain on disk; the package command in `docs/README.md` excludes `src/Task_*/.*` to keep the ZIP clean.
- 2026-08-13 - `sbt clean test assembly` succeeded from `23127442/` after switching to JDK 8. The submission tree itself has no `src/test` sources, so the `test` phase had nothing extra to execute beyond compile/assembly.
- 2026-08-13 - The Python `Task_2-2` `approx` branch uses a deterministic sample-based approximation so the scripts stay stdlib-only and are suitable for CSV comparison, not benchmark reporting.
- 2026-08-13 - Installed Temurin JDK 8 and sbt 1.5.8 directly in the Windows terminal via `winget`; `project/build.properties` already pinned the repo to sbt 1.5.8, so the README prerequisite line was corrected to match reality.
- 2026-08-13 - Downloaded Spark 2.4.8 and Hadoop 3.3.6 runtimes plus `winutils.exe`/`hadoop.dll` to exercise the lab scripts locally on Windows.
- 2026-08-13 - Ran `sbt clean test assembly`, then executed `Task11Main`, `Task12Main`, `Task21Main`, `Task22Main`, and `ValidationMain` against the workspace CSV. The generated `Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`, and `Task_2-2.parquet` all matched the Python reference outputs, and validation passed.

## Surprises / Gotchas

- The shared execution workspace is Windows PowerShell, while the target lab environment described by the user is a Hadoop pseudo-distributed environment. Environment-dependent verification will be separated from code/build checks and never reported as passed without evidence.
- 2026-08-10 - Preflight found no `java`, `scala`, `hadoop`, `hdfs`, `yarn`, `spark-submit`, `sbt`, or `bash` executable on PATH. A portable Java/SBT toolchain may be used for compilation/tests, but it does not substitute for target Hadoop/Spark E2E evidence.
- 2026-08-12 - `sbt` is still missing on PATH in this workspace, so local compile verification for `23127442/` could not be completed here.
- 2026-08-10 - The supplied Amazon CSV has 24 columns, including the trailing `Unnamed: 22`; the checklist draft incorrectly said 23. The parser/checklist use the observed 24-column contract. This does not change the designed `SaleRow` interface.
- 2026-08-10 - Hadoop 3.3.6 Mapper/Reducer overrides from Scala 2.11 require `protected` methods and explicit path-projected `Mapper[...]#Context` / `Reducer[...]#Context` types; inherited `Context` alone does not override the Java signature.
- 2026-08-10 - Hadoop local integration on Windows fails because Hadoop 3.3.6 invokes `NativeIO.Windows.access0` before mapper execution. Docker is absent and WSL has no installed distribution. The discarded placeholder `winutils.exe` / permission-neutral filesystem workaround could not safely emulate this native API, so the integration test now cancels explicitly on Windows and remains executable on Linux/Lab 1. Task 2.7 is honestly marked blocked; no integration pass is claimed.
- 2026-08-10 - The first Spark 2.4 local action aborted with `Incompatible Jackson version: 2.12.7-1`; dependency inspection confirmed Hadoop 3.3.6 had evicted Spark's Jackson 2.6 core while leaving `jackson-module-scala_2.11` 2.6.7.1.
- 2026-08-10 - Spark DataFrame actions run on Windows after the Jackson alignment, including joins and stage collection. Parquet writes still traverse Hadoop 3.3.6 `RawLocalFileSystem` and require the missing native Windows layer, so Parquet persistence tests run on Linux/Lab 1 and cancel explicitly here.
- 2026-08-12 - `23127442/` has no top-level `package-submission.sh`; README is the source of truth for run / validate / package commands.
- 2026-08-12 - PowerShell policy blocked direct directory deletion in this workspace, so the empty legacy task-root folders were renamed to hidden dot-folders to keep the visible tree aligned with the requested layout.
- 2026-08-12 - Because the legacy folders could not be removed, the README package command explicitly excludes hidden task-root dotfolders so the final zip stays clean.
- 2026-08-13 - `Remove-Item -Recurse` on the old task directories is also blocked in this workspace, so the visible `lab3/` folders were renamed to hidden `.lab3/` markers and the README package command now excludes hidden task-root dotfolders.
- 2026-08-13 - The generated `target/scala-2.11/bigdata-lab3.jar` exists and does not bundle `org/apache/spark/` or `org/apache/hadoop/` classes.
- 2026-08-13 - The first Python run exposed a path mismatch because the dataset lives at the workspace root, not inside `23127442/`; the runner now searches the workspace-root path automatically.

## External Inputs Still Required

- The final Google Drive URL is still required before packaging.
