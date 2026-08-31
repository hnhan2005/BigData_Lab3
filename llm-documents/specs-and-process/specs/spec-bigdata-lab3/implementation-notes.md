# Big Data Lab 3 — Implementation Notes

## 2026-08-31 — Đồng bộ theo `Lab3_Slide_ref.pdf`

`rules.md` là bản tổng hợp ngắn gọn, có tính vận hành, của các quyết định bên dưới và đã được link từ toàn bộ spec documents.

### Decisions

- Task 1-1 dùng bought predicate `Status` chứa `shipped` và `Qty > 0`.
- Task 1-1 chọn `Amount` cho population variance theo khuyến nghị trên slide. `Amount=NULL` vẫn được tính vào frequency nhưng không vào moment; variance không xác định được xếp sau variance hữu hạn khi phá hòa.
- Task 1-1 phát bucket từ `t+1` đến `t+L`, nên có thể sinh ngày sau `max(inputDate)` và ngày cuối baseline là 09/07/2022.
- Task 1-2 chỉ dùng bought rows và chọn scope qualifying style toàn cục vì file đáp án của giảng viên dùng cách này. Report phải nêu thêm scope trong từng `(state, month)` vì hai cách lệch 31% nhóm.
- Task 2-1 dùng `Status` chứa `Cancelled` + `ship-service-level=Standard`; mẫu số là toàn bộ Cancelled+Standard rows theo `(state,city)` và phải dùng `LEFT JOIN`.
- Task 2-2 dùng exact nearest-rank `ceil(p*N)`, approximate bằng Spark built-in, và `stddev_pop`/`ddof=0`. Nhóm không đủ Amount hợp lệ xuất `0.0` sau `coalesce`.

### Design deviations

- Task 1-2 design cũ mô tả hai Job và qualification theo `(state,month,style)`. Revision này đổi thành Job A1 tính qualifying style toàn cục, Job A2 tính distinct SKU/variety theo state-month-style, rồi Job B tính median.
- Các acceptance criteria đã từng đánh dấu complete cần được revalidate sau revision; chưa tự động coi code hiện tại đã đáp ứng semantics mới.

### Trade-offs

- Scope XXL toàn cục khớp file đáp án giảng viên nhưng kém trực tiếp hơn cách đọc “trong nhóm”; vì vậy giữ cả hai cách trong evidence/report để người chấm truy nguyên khác biệt.
- Nearest-rank cho threshold nguyên và tái lập được, nhưng có thể khác linear interpolation; comparison phải báo cả threshold difference và final-result difference.
- Không repartition thủ công theo group hiện tại: group lớn nhất chỉ 426 rows/222 KB. Chỉ benchmark hướng giảm 200 shuffle partitions xuống 8–16 hoặc AQE coalesce.

### Surprises / gotchas

- `Status = 'Shipped'` bỏ sót nhiều dòng; phải dùng contains cho Task 1-1.
- Task 2-1 có kết quả 0% là đúng: 6.909 Cancelled+Standard rows đều có promotion rỗng; khi nới ra 18.332 Cancelled rows, mỗi 295 rows có promotion cũng chỉ có một mã.
- Không được dùng `getmerge` để nối Parquet vì có thể làm hỏng footer.

### Follow-up gate

- Người dùng đã xác nhận revision Goals → Design → Checklist cho code execution ngày 2026-08-31.
- Audit đầu execution phát hiện ba drift: `Qty != 0`, Task 1-2 qualification XXL cục bộ, và Task 2-1 dùng exact `Status=CANCELLED`; các điểm này phải sửa trước full run.
- WSL/Docker không khả dụng trên máy hiện tại. Runtime sẽ được dựng tách biệt trong workspace với Java 8 + SBT 1.5.8 + Hadoop/Spark đúng version, chạy local-mode và ghi rõ khác biệt so với pseudo-distributed README.

## 2026-08-31 — Code execution for slide revision

- Files: `Normalization.scala`, `SparkSaleReader.scala`, `GlobalStyleJob.scala`, `VarietyJob.scala`, `Task12Main.scala`, `Task21Job.scala`, `Task21Main.scala`, `PlanEvidence.scala`.
- Summary: Changed bought semantics to `Qty > 0`; implemented Task 1-2 as Job A1 global style eligibility, Job A2 state-month-style distinct SKU variety, then Job B median; changed Task 2-1 status matching to contains and added default/no-broadcast physical-plan evidence.
- Tests: source edits completed; compile, full-data execution and output comparison are in progress.
- Decisions: The qualifying-style set is produced by MapReduce Job A1 and passed to Job A2 through Hadoop configuration because the full-data set is small; this follows the instructor answer-key's global interpretation.
- Blockers: WSL/pseudo-distributed services are unavailable, so verification uses an isolated exact-version local runtime and records this environmental deviation.

### Full-data verification result

- Build: Temurin Java 8 + SBT 1.5.8 `clean test assembly` passed; the minimal submission intentionally contains no test sources, so the test phase discovered zero tests.
- Runtime: Hadoop MapReduce and Spark Parquet writes cannot complete on this Windows host because Hadoop requires `winutils.exe`; no untrusted third-party executable was downloaded. Spark 2.4.8 successfully loaded the query and emitted the updated Task 2-1 physical plan before the filesystem write failed.
- Independent oracle: A workspace-local Python venv (pandas 2.3.2, pyarrow 21.0.0) regenerated/checked all four results. The official Scala/Spark `ValidationMain` then read all four files and passed schema, key and invariant checks.
- Results: Task 1-1 = 3,696 rows through 2022-07-09; Task 1-2 global = 143 rows and local comparison = 128; Task 2-1 = 1,442 state-city groups/6,906 denominator rows/all 0%; Task 2-2 = 16,486 SKU-month groups, maximum 426 rows, 0 groups above 1,000.
- Baseline drift: Task 1-2's slide quick check (128 rows) is the local interpretation and conflicts with the selected global output. Task 2-1 slide values 6,909/1,435 do not reproduce from the current CSV, although the required 0% conclusion does. Goals, Design and `rules.md` were updated before finalizing.
- Remaining target-only check: execute the README commands on Lab 1/WSL with official Hadoop services to obtain genuine MapReduce counters and fresh Spark stage/plan timings; current evidence does not falsely claim that target run.

### Council verification and cleanup

- **SM**: Goals, Design, Checklist, `rules.md` and implementation notes now distinguish selected global output from local slide baselines. Revision semantics are checked off; the full target-runtime evidence item remains in progress, so the main spec stays `inprocess`.
- **PO**: Four deliverables exist and passed `ValidationMain`; the evidence clearly reports matches and mismatches instead of forcing slide values.
- **Dev**: Final Scala 2.11 compile passed for 34 sources after replacing the Task 1-2 configuration control separator with Base64-safe values. `git diff --check` found no whitespace errors.
- Cleanup: Removed the Python venv/runtime copy, temporary input, Hadoop work directories, assembly/classes, SBT `target` caches and the accidental root SBT cache. Kept only source, four outputs and documented evidence.
- Report draft: Created root-level `Report.md` from the user's outline, expanding all four task analyses with formulas, decomposition, complexity, testing, slide comparison and per-section image suggestions; benchmark values were filled from `task22/benchmark-summary.csv`.
- Report revision: Expanded the draft with numbered figure notes for every task, explicit explanations for Task 1-2 global/local differences, Task 2-1 slide-like versus state-city counting, and Task 2-2 percentile baseline differences.
- Report detail expansion: Added input/output contracts, pseudocode, correctness arguments and edge-case handling for Task 1-1, 1-2, 2-1 and 2-2; renumbered subsection and figure references accordingly.
- Report source inventory: Added section 2.5 documenting every preprocessing and shared/support source file; task-specific execution files remain documented only under their corresponding task sections.
