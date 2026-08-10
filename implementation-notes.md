# Big Data Lab 3 — Implementation Notes

> Running log for decisions not covered by the spec, design deviations, trade-offs, and surprises. Update during execution; do not batch at the end.

## Decisions

- 2026-08-10 — Code Execution started after explicit approval of Goals, Design, and Implementation Checklist.
- 2026-08-10 — No `.agents/skills/` directory exists, so there is no local Scala/Hadoop/Spark skill to activate. Skill discovery will be repeated at every phase as required by the checklist.

## Design Deviations

- None at execution start.

## Trade-offs

- 2026-08-10 – Spark 2.4.8 normally brings Hadoop 2.x transitively. The build excludes those transitive Hadoop modules and pins Hadoop 3.3.6 explicitly to avoid two Hadoop generations on the compile/test classpath. Target Spark runtime verification is still required in Lab 1.
- 2026-08-10 – Hadoop 3.3.6 resolves Jackson 2.12 while Spark 2.4.8's `jackson-module-scala` requires Jackson 2.6. The compile/test classpath overrides Jackson core/annotations/databind to Spark-compatible 2.6.7/2.6.7.3. These runtime libraries remain `provided` transitively and are not bundled; the Lab 1 `spark-submit` distribution remains authoritative and must pass preflight/E2E.

## Surprises / Gotchas

- The shared execution workspace is Windows PowerShell, while the target lab environment described by the user is a Hadoop pseudo-distributed environment. Environment-dependent verification will be separated from code/build checks and never reported as passed without evidence.
- 2026-08-10 — Preflight found no `java`, `scala`, `hadoop`, `hdfs`, `yarn`, `spark-submit`, `sbt`, or `bash` executable on PATH. A portable Java/SBT toolchain may be used for compilation/tests, but it does not substitute for target Hadoop/Spark E2E evidence.
- 2026-08-10 — The supplied Amazon CSV has 24 columns, including the trailing `Unnamed: 22`; the checklist draft incorrectly said 23. The parser/checklist use the observed 24-column contract. This does not change the designed `SaleRow` interface.
- 2026-08-10 — Hadoop 3.3.6 Mapper/Reducer overrides from Scala 2.11 require `protected` methods and explicit path-projected `Mapper[...]#Context` / `Reducer[...]#Context` types; inherited `Context` alone does not override the Java signature.
- 2026-08-10 – Hadoop local integration on Windows fails because Hadoop 3.3.6 invokes `NativeIO.Windows.access0` before mapper execution. Docker is absent and WSL has no installed distribution. The discarded placeholder `winutils.exe`/permission-neutral filesystem workaround could not safely emulate this native API, so the integration test now cancels explicitly on Windows and remains executable on Linux/Lab 1. Task 2.7 is honestly marked blocked; no integration pass is claimed.
- 2026-08-10 – The first Spark 2.4 local action aborted with `Incompatible Jackson version: 2.12.7-1`; dependency inspection confirmed Hadoop 3.3.6 had evicted Spark's Jackson 2.6 core while leaving `jackson-module-scala_2.11` 2.6.7.1.
- 2026-08-10 – Spark DataFrame actions run on Windows after the Jackson alignment, including joins and stage collection. Parquet writes still traverse Hadoop 3.3.6 `RawLocalFileSystem` and require the missing native Windows layer, so Parquet persistence tests run on Linux/Lab 1 and cancel explicitly here.

## External Inputs Still Required

- Actual target `spark-submit --version` output if it is not available in this workspace.
- Representative student ID is now confirmed as `23127442`; only the final Google Drive URL is still required before packaging.
- A Lab 1 run of `scripts/run-all.sh` to generate/validate the four final outputs and replace every `CẦN ĐIỀN` marker in `docs/Report.md`, followed by PDF regeneration and `package-submission.sh`.
