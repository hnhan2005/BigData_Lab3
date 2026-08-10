# Spec Big Data Lab 3 — Advanced MapReduce & Spark Structured APIs

> **Nguồn yêu cầu**: [`Lab 3 - MR-Spark.pdf`](../../../../Lab%203%20-%20MR-Spark.pdf)
>
> **Tài liệu liên quan**:
> - [Detailed Goals](./spec-bigdata-lab3-detailed-goal.md)
> - [Detailed Design](./spec-bigdata-lab3-detailed-design.md)
> - [Implementation Checklist](./spec-bigdata-lab3-implementation-checklist.md)

## Spec Goal

Xây dựng một bài nộp nhóm thống nhất, chạy được bằng Scala trên môi trường Hadoop/Spark cục bộ đã cài từ Lab 1, giải đúng bốn bài toán trong đề bằng MapReduce và Spark Structured APIs, xuất đúng bốn tệp kết quả trên filesystem thông thường, đồng thời cung cấp báo cáo phân tích và README tiếng Việt đủ để chạy lại từng bước từ terminal.

## Spec Stories

- **Bài 1.1 — MapReduce sliding window động**:
  - Là thành viên nhóm, tôi muốn tìm size được mua nhiều nhất theo state và ngày cửa sổ với độ dài cửa sổ động, để đáp ứng đúng thuật toán map-to-buckets và quy tắc phá hòa trong đề.
- **Bài 1.2 — MapReduce median variety**:
  - Là thành viên nhóm, tôi muốn tính median variety theo state và tháng cho các style phục vụ size từ XXL trở lên, để đo độ đa dạng SKU trong từng khoảng không-thời gian.
- **Bài 2.1 — Spark Structured APIs và phân tích execution plan**:
  - Là thành viên nhóm, tôi muốn tính tỷ lệ đơn hủy Standard theo city thỏa điều kiện promotion và amount, để có kết quả đúng cùng bằng chứng `explain(true)`, join, shuffle và stage.
- **Bài 2.2 — Spark percentile gần đúng và chính xác**:
  - Là thành viên nhóm, tôi muốn tính độ lệch chuẩn amount theo SKU-tháng tại ngưỡng P90/P80 bằng hai cách, để so sánh độ chính xác, thời gian và tập đơn hàng đạt ngưỡng.
- **Khả năng tái chạy và bài nộp**:
  - Là người chấm bài, tôi muốn mã Scala có chú thích, kết quả đúng định dạng, báo cáo đầy đủ và README tiếng Việt theo từng lệnh terminal, để có thể kiểm tra và tái lập kết quả trên môi trường được cho phép.

## Spec Planning

- **Ngày bắt đầu**: 2026-08-10
- **Ngày kết thúc**: Chưa xác định theo hạn Moodle
- **Capacity**: 34 Story Points, ước tính khoảng 45–60 giờ công nhóm bao gồm code, test, full-data runs, evidence, report và packaging
- **Môi trường mục tiêu đã biết**:
  - Hadoop 3.3.6, pseudo-distributed
  - Java 8
  - Scala 2.11.12
  - Chỉ dùng môi trường cài từ Lab 1; không dùng Google Colab
- **Dữ liệu đầu vào được cung cấp**:
  - `Amazon Sale Report.csv` — 68,923,428 byte, 128,976 dòng tính cả header
  - `shapes.parquet(legacy)` — tệp tham chiếu/legacy cần xác định vai trò ở phase Design
- **Testing**:
  - Kiểm thử logic bằng các fixture nhỏ có kết quả tính tay cho cả bốn bài.
  - Chạy end-to-end trên toàn bộ CSV được cung cấp trong môi trường mục tiêu.
  - Xác minh tệp CSV/Parquet cuối có thể đọc từ filesystem thông thường bằng công cụ độc lập phù hợp.
  - Mọi benchmark phải chạy ít nhất 5 lần và báo cáo mean cùng standard deviation.
- **Rủi ro**:
  - **Thiếu thông tin Spark**: Chưa biết phiên bản/cách cài Spark tương thích với Scala 2.11.12 và Java 8.
  - **Một số cách hiểu chưa được đề định nghĩa tuyệt đối**: Mẫu số của tỷ lệ, quy ước exact percentile, phạm vi ngày phát sinh của sliding window và một số điều kiện lọc cần được xác nhận hoặc nêu giả định minh bạch trong báo cáo.
  - **“Single file” trên output phân tán**: MapReduce/Spark thường ghi thư mục gồm part files; thiết kế phải tạo đúng một tệp vật lý trên filesystem thường với filename bắt buộc.
  - **Dữ liệu CSV có null và chuỗi promotion dài**: Parser, schema, chuẩn hóa ngày/size và xử lý null phải được xác định rõ ở Design.
- **Cam kết**:
  - Ưu tiên Scala để đạt trọn điểm ngôn ngữ; không triển khai lời giải chính bằng Python hoặc Java.
  - MapReduce dùng đúng framework MapReduce và bài 1.1 dùng map-to-buckets theo đề.
  - Spark chỉ dùng DataFrame/Dataset API cho lời giải được chấm; không dùng SQL query string.
  - Chỉ fallback sang native Scala/library khi Spark built-in APIs không đủ, và phải giải thích.
  - Không bổ sung yêu cầu ngoài đề nếu không cần cho tính đúng, khả năng chạy hoặc khả năng kiểm chứng.

## Phase Approvals

- **Detailed Goals**: Approved — người dùng xác nhận ngày 2026-08-10
- **Detailed Design**: Approved — người dùng xác nhận ngày 2026-08-10
- **Implementation Checklist**: Approved — người dùng xác nhận ngày 2026-08-10

## During Spec

- **Standups**: Chưa bắt đầu thực thi.
- **Impediments**:
  - Chưa có RepresentativeID, deadline Moodle và thông tin phiên bản/cài đặt Spark.
  - Workspace thực thi hiện không có Java/Scala/Hadoop/Spark/SBT/Bash trên PATH; cần portable build tools cho compile/test và vẫn cần target Lab 1 để chạy pseudo-distributed E2E.
- **Adjustments**:
  - Tên spec theo yêu cầu người dùng là `bigdata-lab3`; tài liệu nguồn tự gọi bài là “Lab 02/Lab 2”. Các yêu cầu kỹ thuật, grading và cấu trúc nộp trong PDF vẫn được coi là nguồn sự thật.
  - 2026-08-10: Detailed Goals được người dùng phê duyệt; spec chuyển sang phase Design.
  - 2026-08-10: Detailed Design được người dùng phê duyệt; spec chuyển sang phase Implementation Checklist.
  - 2026-08-10: Implementation Checklist được người dùng phê duyệt; spec chuyển sang Code Execution.

## Spec Review

- **Completed**: Chưa bắt đầu.
- **Demo**: Chưa có.
- **Feedback**: Chưa có.

## Spec Retrospective

- **Well**: Sẽ cập nhật sau thực thi.
- **Not Well**: Sẽ cập nhật sau thực thi.
- **Improvements**: Sẽ cập nhật sau thực thi.

## Next Spec Adjustments

- **Changes**: Sẽ cập nhật sau review.
- **Carry-over**: Chưa có.
- **Lessons**: Sẽ cập nhật sau review.
