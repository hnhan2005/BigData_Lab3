# Detailed Goals — Big Data Lab 3

> **Reference**: [Main Spec File](./spec-bigdata-lab3-inprocess.md)
>
> **Nguồn yêu cầu gốc**: [`Lab3_Slide_ref.pdf`](../../../../Lab3_Slide_ref.pdf)
>
> **Rules thực thi**: [rules.md](./rules.md)

## Spec Goal
Revision note 2026-08-12: this spec now also requires a WSL-friendly README with `<user_name>`/`$HOME` placeholders, a flattened task source layout under `src/Task_*`, and no top-level `scripts/` directory in the final submission.

Tạo đầy đủ artefact có thể chấm và tái lập cho bốn bài toán Advanced MapReduce và Spark Structured APIs bằng Scala, tuân thủ chính xác điều kiện lọc/tổng hợp, thuật toán bắt buộc, định dạng output, nội dung báo cáo và cấu trúc nộp bài do giảng viên quy định.

## 1. Bối cảnh và giá trị

### Cập nhật từ slide tham chiếu 2026-08-31

Các số liệu dưới đây là baseline dùng để kiểm tra full-data và viết Report:

- Input có 128.975 dòng dữ liệu, ngày từ 31/03/2022 đến 29/06/2022.
- Sau chuẩn hóa `UPPER(TRIM(state))` có 46 state.
- `Status` phải kiểm tra bằng điều kiện chứa `shipped`, không dùng phép bằng chuỗi `Shipped`.
- Size phải được xếp theo rank nghiệp vụ: `XS=1, S=2, M=3, L=4, XL=5, XXL=6, 3XL=7, ...`; “ít nhất XXL” là `rank >= 6`.
- CSV phải được parse đúng quote; `promotion-ids` có dấu phẩy bên trong.
- `Qty` và `Amount` có null/giá trị biên; mọi phép so sánh hoặc aggregate phải có null policy tường minh.

Các kết quả kiểm tra chính: Task 1-1 có 3.696 dòng và ngày cuối 09/07/2022; Task 1-2 output global đã chọn có 143 dòng (đối chiếu local có 128 dòng); Task 2-1 phải cho 0% ở mọi city khi giữ nguyên điều kiện đề.

Spec biến đề bài PDF thành các yêu cầu quan sát và kiểm thử được trước khi chọn kiến trúc. Kết quả cuối không chỉ cần “chạy ra số”, mà còn phải chứng minh cách hiểu truy vấn, cách phân rã, lý do thiết kế, độ đúng của kết quả và khả năng chạy lại trên môi trường Lab 1.

### Vai trò người dùng

- **Thành viên nhóm**: xây dựng, kiểm thử, báo cáo và đóng gói một lời giải thống nhất.
- **Đại diện nhóm**: chạy toàn bộ pipeline, tải output lên Drive và nộp ZIP lên Moodle.
- **Giảng viên/người chấm**: đọc phân tích, chạy lại mã và kiểm tra bốn output.

## 2. Phạm vi dữ liệu và lưu trữ

- **Dữ liệu đọc**: `Amazon Sale Report.csv` được cung cấp kèm đề; tên thực tế trong workspace khác tên `asr.csv` được nhắc ở chú thích PDF, nên cách truyền đường dẫn đầu vào phải được README làm rõ.
- **Schema quan trọng đã quan sát**: `Order ID`, `Date`, `Status`, `Fulfilment`, `ship-service-level`, `Style`, `SKU`, `Size`, `Courier Status`, `Qty`, `Amount`, `ship-city`, `ship-state`, `promotion-ids` cùng các cột bổ trợ.
- **Đặc điểm dữ liệu**: có giá trị trống/null; ngày mẫu ở dạng `MM-dd-yy`; `promotion-ids` có thể chứa danh sách phân cách bằng dấu phẩy và gồm cả promotion do Amazon phát hành.
- **Dữ liệu ghi**: chính xác bốn kết quả cuối là `Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`, `Task_2-2.parquet`.
- **Quyền sở hữu/lifecycle**: file đầu vào do đề cung cấp, không bị sửa; output được tái tạo khi chạy và được đặt trên filesystem thông thường để tải lên Google Drive.
- **Tính nhất quán**: cùng input và tham số phải cho cùng nội dung logic; thứ tự dòng chỉ được coi là yêu cầu nếu schema/output contract ở Design quy định rõ để kiểm tra tái lập.
- **Nhu cầu truy vấn**: group theo state/city/SKU/style/month/window date; lọc status, service level, fulfilment, courier status, size, số promotion và amount; tính count, distinct count, median, percentile, variance và population standard deviation.
- **Quy mô hiện tại**: CSV có 128,975 bản ghi dữ liệu (128,976 dòng tính cả header), dung lượng 68,923,428 byte. Thiết kế không được giả định dữ liệu vừa bộ nhớ của một mapper/executor nếu đề yêu cầu xử lý phân tán.
- **Migration/backfill**: không có database hoặc migration; output được sinh mới từ toàn bộ dataset.
- **Audit/bằng chứng**: log lệnh chạy, kiểm thử, execution plan và số liệu benchmark phải đủ để đưa vào báo cáo; không có yêu cầu audit người dùng hoặc PII ngoài phạm vi bài lab.

## 3. Yêu cầu chung bắt buộc

### R-GEN-01 — Ngôn ngữ và môi trường

**User Story:** Là thành viên nhóm, tôi muốn lời giải chạy bằng Scala trên môi trường Lab 1, để đạt đầy đủ điểm ngôn ngữ và không vi phạm giới hạn môi trường.

#### Acceptance Criteria

1. WHEN xây dựng lời giải chính THEN nhóm SHALL dùng Scala; Java/Python không được dùng làm lời giải thay thế cho bốn bài.
2. WHEN chạy bài lab THEN lời giải SHALL chạy trong môi trường cục bộ/pseudo-distributed đã cài từ Lab 1 và SHALL NOT phụ thuộc Google Colab.
3. IF Spark built-in APIs đủ để thực hiện một bước THEN lời giải SHALL dùng built-in API thay vì fallback sang native code/library.
4. IF bắt buộc fallback sang native Scala/library THEN báo cáo SHALL chỉ rõ bước đó, lý do built-in API không đủ và phạm vi fallback.
5. WHEN cung cấp mã nguồn THEN mã SHALL có comment rõ ràng cho các phần xử lý không hiển nhiên.

### R-GEN-02 — Khả năng chạy lại và README tiếng Việt
Revision note 2026-08-12: README must target WSL, use `<user_name>`/`$HOME` placeholders, and describe the full install -> config -> build -> run -> validate -> package sequence directly in terminal commands rather than pointing to a `/scripts` directory.

**User Story:** Là người chấm, tôi muốn README tiếng Việt mô tả lệnh terminal theo từng bước, để có thể thiết lập, chạy và kiểm tra từng task.

#### Acceptance Criteria

1. WHEN bàn giao THEN `docs/README.md` SHALL được viết bằng tiếng Việt dù README là optional trong PDF, vì đây là yêu cầu bổ sung của người dùng.
2. WHEN mô tả mỗi lệnh THEN README SHALL đặt lệnh trong code block, theo đúng thứ tự chạy, kèm chú thích ngắn gọn về mục đích và output mong đợi.
3. WHEN mô tả môi trường THEN README SHALL nêu prerequisite và lệnh kiểm tra Java 8, Hadoop 3.3.6, Scala 2.11.12, Spark cùng trạng thái HDFS/YARN cần thiết theo thiết kế được duyệt.
4. WHEN mô tả từng task THEN README SHALL có lệnh build, chuẩn bị input, chạy, thu kết quả về filesystem thường và kiểm tra output.
5. WHEN một giá trị phụ thuộc người nộp/máy THEN README SHALL dùng placeholder rõ nghĩa như `<RepresentativeID>`, `<SPARK_HOME>`, `<INPUT_CSV>` và giải thích cách thay.

### R-GEN-03 — Benchmark

**User Story:** Là người chấm, tôi muốn benchmark có phương pháp thống kê đúng, để so sánh hiệu năng có ý nghĩa.

#### Acceptance Criteria

1. WHEN một phép đo được gọi là benchmark THEN hệ thống SHALL chạy ít nhất 5 lần.
2. WHEN báo cáo benchmark THEN báo cáo SHALL nêu từng phép đo, arithmetic mean và standard deviation trên các lần chạy.
3. WHEN so sánh approximate và exact percentile ở Task 2-2 THEN cả hai cách SHALL được đo trong điều kiện so sánh tương đương và tuân thủ AC 1–2.
4. IF nhóm bổ sung benchmark khác ngoài yêu cầu tối thiểu THEN benchmark đó SHALL tuân thủ cùng quy tắc ít nhất 5 lần, mean và standard deviation.

## 4. Task 1-1 — MapReduce dynamic-length sliding window

### R-MR-11-01 — Xác định bought order và độ dài cửa sổ theo state

**User Story:** Là thành viên nhóm, tôi muốn xác định đúng đơn đã mua và cửa sổ của từng state, để mọi bucket dùng đúng quy tắc động.

#### Acceptance Criteria

1. WHEN `Status` chứa từ “shipped” và `Qty > 0` THEN record/order SHALL được coi là “bought”; không coi `Qty = 0` là bought và so khớp status SHALL xử lý nhất quán khác biệt hoa/thường.
2. WHEN tính tổng bought orders theo state THEN phép tính SHALL dùng toàn bộ dataset, không chỉ phạm vi của một cửa sổ.
3. IF một state có tổng bought orders lớn hơn 10,000 THEN độ dài cửa sổ của state đó SHALL là 5 ngày.
4. IF một state có tổng bought orders nhỏ hơn hoặc bằng 10,000 THEN độ dài cửa sổ của state đó SHALL là 10 ngày.
5. WHEN đánh giá ngày hiện tại `d` với cửa sổ 5 ngày THEN cửa sổ SHALL bao gồm `d-5` đến `d-1` và SHALL NOT bao gồm `d`.
6. WHEN đánh giá ngày hiện tại `d` với cửa sổ 10 ngày THEN cửa sổ SHALL bao gồm `d-10` đến `d-1` và SHALL NOT bao gồm `d`.
7. WHEN ở gần đầu miền ngày và không có đủ lịch sử THEN cửa sổ SHALL được phép ngắn hơn, không tạo dữ liệu mua giả để lấp ngày.
8. WHEN kiểm tra full-data THEN chỉ `MAHARASHTRA` (19.103 bought orders) và `KARNATAKA` (14.950 bought orders) SHALL dùng cửa sổ 5 ngày; các state còn lại SHALL dùng 10 ngày.

### R-MR-11-02 — Map-to-buckets và bước trượt một ngày

**User Story:** Là thành viên nhóm, tôi muốn gán mỗi record vào mọi bucket cửa sổ liên quan, để triển khai đúng chiến lược bắt buộc của đề.

#### Acceptance Criteria

1. WHEN mapper xử lý một bought record THEN lời giải SHALL map record đó vào tất cả window buckets `(state, window_date, size)` mà record thuộc về theo window length của state.
2. WHEN các bucket được tổng hợp THEN combine/reduce SHALL hoạt động theo khóa logic `(state, window_date, size)` trước khi chọn size thắng ở `(state, window_date)`.
3. WHEN cửa sổ tiến triển THEN `window_date` SHALL trượt chính xác 1 ngày mỗi bước.
4. WHEN một `window_date` không xuất hiện trong timestamp đầu vào nhưng được sinh hợp lệ bởi sliding window THEN kết quả SHALL cho phép timestamp đó xuất hiện.
5. WHEN kiểm thử fixture nhỏ THEN tập bucket nhận mỗi record SHALL khớp tập ngày được tính tay, gồm cả biên đầu/cuối miền ngày đã thống nhất.

### R-MR-11-03 — Chọn size và phá hòa

**User Story:** Là người chấm, tôi muốn size thắng được chọn theo đúng frequency, variance và thứ tự từ điển, để kết quả xác định và đúng đề.

#### Acceptance Criteria

1. WHEN chỉ một size có frequency bought orders cao nhất trong cửa sổ THEN hệ thống SHALL chọn size đó.
2. WHEN từ hai size trở lên đồng hạng frequency cao nhất THEN hệ thống SHALL chọn size có population variance của purchased amount thấp nhất trong đúng cửa sổ đó.
3. WHEN tính population variance THEN mẫu số SHALL là `N`, không phải `N-1`.
4. IF frequency và variance vẫn hòa THEN hệ thống SHALL chọn size nhỏ nhất theo lexicographical order của chuỗi, ví dụ `L < M < S < XL < XXL` theo so sánh từ điển nêu trong đề.
5. WHEN amount có null/không hợp lệ hoặc nhóm không đủ giá trị THEN chính sách xử lý SHALL được nêu rõ, kiểm thử và không được âm thầm thay đổi frequency; phương sai dùng `Amount` theo quyết định của spec.
6. WHEN frequency, variance và lexical key đều được tính THEN tie-break SHALL dùng `>` thay vì `>=` khi secondary sort đã đưa size về thứ tự A-Z.

### R-MR-11-04 — Output và báo cáo Task 1-1

**User Story:** Là người chấm, tôi muốn một CSV cùng phân tích thuật toán đầy đủ, để kiểm tra cả kết quả và tư duy MapReduce.

#### Acceptance Criteria

1. WHEN Task 1-1 hoàn tất THEN kết quả SHALL là đúng một tệp vật lý `Task_1-1.csv` đọc được từ filesystem thông thường, không phải tập Hadoop part files.
2. WHEN chọn schema output THEN schema SHALL đủ để nhận diện tối thiểu state, window date, size thắng và các số liệu cần thiết để kiểm chứng; schema chính xác được chốt ở Design.
3. WHEN viết Report THEN phần Task 1-1 SHALL mô tả key design và cách mỗi record được gán vào window buckets.
4. WHEN viết Report THEN phần Task 1-1 SHALL phân tích theoretical time complexity, bao gồm cách tiếp cận tránh/giảm chi phí naive `O(n × w)` nếu áp dụng được.
5. WHEN viết Report THEN phần Task 1-1 SHALL phân tích shuffle complexity/lượng dữ liệu truyền giữa map và reduce.
6. WHEN viết Report THEN phần Task 1-1 SHALL giải thích và biện minh thứ tự tie-breaking frequency → lower population variance → lexicographical smallest.
7. WHEN kiểm tra full-data THEN output SHALL có 3.696 dòng, ngày lớn nhất `2022-07-09`, và size `M` SHALL thắng nhiều nhất (1.299 lần).
8. WHEN phân tích hiệu năng THEN Report SHALL so sánh ba mức shuffle: naive 925.395 phiếu, mảng hiệu 219.132 phiếu và combiner 27.134 phiếu.

## 5. Task 1-2 — MapReduce state-level median variety

### R-MR-12-01 — Variety theo style, state và tháng

**User Story:** Là thành viên nhóm, tôi muốn tính variety trong đúng không-thời gian, để median phản ánh số SKU khác nhau của mỗi style.

#### Acceptance Criteria

1. WHEN nhóm record theo một tháng lịch và một state THEN mỗi tháng SHALL chạy từ ngày đầu đến ngày cuối thực tế của tháng đó, ví dụ 07-01 đến 07-31.
2. WHEN tính variety của một style trong `(state, month)` THEN variety SHALL bằng số `SKU` phân biệt gắn với style đó trong đúng interval và region.
3. WHEN cùng SKU xuất hiện lặp lại cho cùng style/state/month THEN SKU đó SHALL chỉ đóng góp 1 vào variety.
4. WHEN một style được xét cho median THEN style đó SHALL có phục vụ ít nhất một size thuộc ngưỡng “at least XXL”, gồm ví dụ `XXL`, `3XL`, `4XL`, v.v.
5. WHEN chuẩn hóa size tương đương như `XXXL` và `3XL` hoặc gặp size không nhận diện được THEN quy tắc SHALL được định nghĩa minh bạch trong Design và có fixture kiểm thử.
6. WHEN xác định style đạt ngưỡng XXL THEN scope SHALL là toàn cục: style đã từng bán size `>= XXL` ở bất kỳ state/tháng nào được coi là qualifying; Report SHALL đồng thời nêu cách hiểu trong từng `(state, month)` vì slide xác nhận hai cách lệch 40/128 nhóm (31%).

### R-MR-12-02 — Median cấp state

**User Story:** Là người chấm, tôi muốn median variety chính xác cho mỗi state-tháng, để kết quả không bị lệch bởi cách xử lý số lượng style chẵn/lẻ.

#### Acceptance Criteria

1. WHEN có các variety của mọi style đủ điều kiện trong `(state, month)` THEN hệ thống SHALL tính median trên toàn bộ tập giá trị đó.
2. WHEN số style đủ điều kiện là lẻ THEN median SHALL là giá trị chính giữa sau khi sắp xếp.
3. WHEN số style đủ điều kiện là chẵn THEN quy tắc median SHALL dùng trung bình số học của hai giá trị giữa, theo quy ước trên slide.
4. WHEN không có style đủ điều kiện trong một `(state, month)` THEN chính sách có/không phát output SHALL được chốt và kiểm thử trước triển khai.
5. WHEN kiểm tra full-data THEN output chính theo global+bought SHALL có 143 nhóm `(month,state)`; evidence local+bought SHALL có 128 nhóm. `MAHARASHTRA` tháng 04/2022 lần lượt là 863 style/median 3,0 và 621 style/median 4,0; mốc slide 647 style phải được ghi là không tái lập dưới bought predicate đã chọn.

### R-MR-12-03 — Output và báo cáo Task 1-2

**User Story:** Là người chấm, tôi muốn một CSV và giải thích decomposition rõ ràng, để có thể đối chiếu truy vấn với implementation.

#### Acceptance Criteria

1. WHEN Task 1-2 hoàn tất THEN kết quả SHALL là đúng một tệp vật lý `Task_1-2.csv` đọc được từ filesystem thông thường.
2. WHEN chọn schema output THEN schema SHALL nhận diện tối thiểu state, month và median variety.
3. WHEN viết Report THEN phần Task 1-2 SHALL giải thích cách hiểu truy vấn, phân rã thành các bước elemental, chiến lược triển khai từng bước và lý do của decomposition.
4. WHEN kiểm thử THEN fixture SHALL bao phủ SKU lặp, nhiều style, tháng có số style chẵn/lẻ và các biểu diễn size từ XXL trở lên.

## 6. Task 2-1 — Spark Structured APIs: cancelled Standard orders

### R-SP-21-01 — Xác định promotion hợp lệ theo thời gian

**User Story:** Là thành viên nhóm, tôi muốn xác định promotion hợp lệ từ toàn dataset, để đếm đúng các promotion của từng order.

#### Acceptance Criteria

1. WHEN đọc `promotion-ids` THEN hệ thống SHALL tách tất cả promotion identifiers gắn với order, gồm cả promotion do Amazon phát hành.
2. WHEN xét mỗi identifier duy nhất trên toàn bộ orders THEN hệ thống SHALL tính ngày xuất hiện đầu tiên và cuối cùng.
3. WHEN tính active period THEN hệ thống SHALL dùng số ngày giữa first appearance date và last appearance date.
4. IF active period lớn hơn hoặc bằng 2 ngày THEN promotion SHALL được coi là temporally valid.
5. IF promotion trống/null THEN nó SHALL không được tính là một identifier.
6. WHEN một order có identifier lặp do dữ liệu/parse THEN chính sách đếm SHALL không làm tăng sai số promotion duy nhất gắn với order và phải được kiểm thử.
7. WHEN kiểm tra full-data THEN có 284 promotion identifiers, trong đó 185 mã có active period `>= 2` ngày; promotion do Amazon phát hành SHALL vẫn được giữ.

### R-SP-21-02 — State average làm ngưỡng amount

**User Story:** Là thành viên nhóm, tôi muốn tính average amount đúng tập merchant-fulfillment shipped, để so sánh từng cancelled order với ngưỡng state liên quan.

#### Acceptance Criteria

1. WHEN tạo tập tham chiếu average THEN hệ thống SHALL chỉ dùng orders có `Fulfilment` là Merchant và `Courier Status` là `Shipped`, theo chính sách chuẩn hóa hoa/thường được chốt.
2. WHEN tính average THEN hệ thống SHALL tính average purchased amount riêng cho từng state.
3. WHEN một cancelled order được so sánh THEN hệ thống SHALL dùng average của chính associated state.
4. IF state không có average hợp lệ hoặc order amount null/không hợp lệ THEN chính sách loại/giữ SHALL được chốt rõ và kiểm thử, không được mặc định amount bằng 0 nếu không có căn cứ.
5. WHEN kiểm tra full-data THEN state average SHALL được tính trên các đơn `Fulfilment=Merchant` và `Courier Status=Shipped`, tạo 40 state thresholds.

### R-SP-21-03 — Tỷ lệ theo city

**User Story:** Là người chấm, tôi muốn tỷ lệ theo city phản ánh đúng toàn bộ điều kiện, để output có thể tái tính độc lập.

#### Acceptance Criteria

1. WHEN tạo tử số cho một city THEN order SHALL đồng thời có status Cancelled, service level Standard, ít nhất 3 temporally-valid promotions và amount nhỏ hơn state merchant-fulfillment/Shipped average.
2. WHEN tính tỷ lệ THEN numerator, denominator và hệ số biểu diễn phần trăm SHALL dùng một định nghĩa được nêu công khai trong Report và schema output.
3. WHEN city/state khác nhau về hoa/thường hoặc khoảng trắng THEN chính sách chuẩn hóa SHALL tránh chia tách nhóm giả và không làm mất giá trị gốc cần báo cáo.
4. WHEN denominator bằng 0 THEN hệ thống SHALL không chia cho 0 và SHALL áp dụng chính sách output đã chốt.
5. WHEN kiểm thử fixture THEN tỷ lệ SHALL được đối chiếu bằng phép tính tay cho city có order đạt/không đạt và state có/không có average.
6. WHEN tính mẫu số THEN phải giữ cả order không có promotion bằng `LEFT JOIN`; slide nêu 6.909 đơn/1.435 city, còn CSV hiện tại sau parser chuẩn cho 6.906 đơn/1.442 `(state,city)` groups.
7. WHEN kiểm tra full-data THEN percentage SHALL bằng 0% ở mọi city; Report SHALL nêu rõ chênh lệch baseline, đồng thời chứng minh 18.332 Cancelled orders chỉ có 295 order có promotion và mỗi order chỉ có 1 mã.

### R-SP-21-04 — Ràng buộc API, execution plan và output

**User Story:** Là người chấm, tôi muốn lời giải dùng Structured APIs cùng phân tích plan, để đánh giá đúng kiến thức Spark thay vì câu SQL string.

#### Acceptance Criteria

1. WHEN triển khai lời giải được chấm THEN mã SHALL chỉ dùng Spark DataFrame/Dataset API và SHALL NOT dùng direct Spark SQL string query.
2. WHEN hoàn tất query THEN Report SHALL chứa output đầy đủ của `explain(true)` hoặc extended execution plan tương đương.
3. WHEN phân tích physical plan THEN Report SHALL chỉ ra join strategy Spark thực sự chọn, ví dụ BroadcastHashJoin, SortMergeJoin hoặc BroadcastNestedLoopJoin.
4. WHEN phân tích physical plan THEN Report SHALL đếm và giải thích số shuffle exchanges (`Exchange` nodes).
5. WHEN chạy query THEN Report SHALL nêu số stages thực tế và phương pháp quan sát/đếm.
6. WHEN Task 2-1 hoàn tất THEN kết quả SHALL là đúng một tệp vật lý `Task_2-1.parquet` trên filesystem thông thường, đọc được bằng Pandas hoặc Spark local mode.
7. WHEN chọn schema output THEN schema SHALL đủ để xác định city, numerator, denominator và percentage hoặc các trường tương đương để kiểm chứng.
8. WHEN viết Report THEN phần Task 2-1 SHALL giải thích cách hiểu, decomposition, chiến lược implementation và lý do từng bước.
9. WHEN phân tích plan mặc định THEN Report SHALL đối chiếu 4 `Exchange`, 3 `BroadcastHashJoin`, 0 `Sort`; khi tắt broadcast bằng `autoBroadcastJoinThreshold=-1`, đối chiếu 7 `Exchange`, 3 `SortMergeJoin`, 6 `Sort`.

## 7. Task 2-2 — Spark Structured APIs: dynamic percentiles

### R-SP-22-01 — Promotion count và nhóm SKU-month

**User Story:** Là thành viên nhóm, tôi muốn đếm promotion của mỗi order và nhóm đúng SKU-month, để hai percentile dùng chung đầu vào.

#### Acceptance Criteria

1. WHEN tính promotion count của order THEN hệ thống SHALL đếm tất cả promotion identifiers gắn với order, gồm promotion do Amazon phát hành.
2. IF order không có promotion THEN promotion count SHALL bằng 0.
3. WHEN nhóm THEN percentile threshold SHALL được tính độc lập cho từng `(SKU, calendar month)`.
4. WHEN một Order ID xuất hiện trên nhiều record THEN grain của “order” và cách tránh double count SHALL được chốt trong Design, ghi trong Report và kiểm thử.
5. WHEN kiểm tra full-data THEN có 16.486 nhóm `(SKU, month)`; promotion count SHALL nằm trong khoảng 0–26 và ô rỗng SHALL có count bằng 0.

### R-SP-22-02 — P90/P80 và population standard deviation

**User Story:** Là người chấm, tôi muốn cả P90 và P80 lọc động theo từng nhóm, để độ lệch chuẩn amount đúng yêu cầu.

#### Acceptance Criteria

1. WHEN tính P90 cho `(SKU, month)` THEN hệ thống SHALL chọn orders có promotion count lớn hơn hoặc bằng 90th-percentile threshold của chính nhóm đó.
2. WHEN tính P80 THEN hệ thống SHALL áp dụng logic tương tự với 80th-percentile threshold.
3. WHEN tính độ lệch chuẩn amount của orders đạt ngưỡng THEN hệ thống SHALL dùng population standard deviation với degrees of freedom bằng 0.
4. IF một `(SKU, month, percentile level)` có ít hơn 2 qualifying orders THEN standard deviation SHALL bằng 0.
5. WHEN xuất kết quả THEN P90 và P80 SHALL đều có threshold, số qualifying orders và standard deviation hoặc schema tương đương đủ để kiểm chứng.
6. WHEN amount null/không hợp lệ THEN chính sách xử lý SHALL thống nhất giữa approximate/exact và được nêu trong Report.
7. IF nhóm sau lọc còn 0 hoặc 1 giá trị `Amount` hợp lệ THEN standard deviation SHALL được xuất là `0.0` bằng `coalesce`; không biến `Amount=NULL` thành 0 trước khi tính trung bình.

### R-SP-22-03 — Hai cách tính percentile

**User Story:** Là thành viên nhóm, tôi muốn triển khai percentile gần đúng và chính xác bằng Structured APIs, để so sánh đúng theo đề.

#### Acceptance Criteria

1. WHEN tính approximate percentile THEN lời giải SHALL dùng Spark built-in `approx_percentile` hoặc `percentile_approx` khả dụng trong phiên bản Spark mục tiêu.
2. WHEN tính exact percentile THEN lời giải SHALL tự triển khai bằng DataFrame/Dataset operations, SHALL NOT dùng direct Spark SQL string query và SHALL NOT giả mạo exact bằng hàm approximate với accuracy cao.
3. WHEN định nghĩa exact percentile THEN quy ước rank/interpolation SHALL được ghi rõ, áp dụng nhất quán cho P90/P80 và xác nhận trước triển khai.
4. WHEN hai cách chạy trên cùng input THEN chúng SHALL dùng cùng cách parse, grain order, group key, null policy và filter `>= threshold`.
5. WHEN chọn exact percentile THEN spec SHALL dùng nearest-rank `ceil(p*N)` để lấy một promotion count quan sát được; linear interpolation SHALL chỉ là phương án đối chiếu, không phải kết quả chính.

### R-SP-22-04 — So sánh accuracy, runtime và qualifying sets

**User Story:** Là người chấm, tôi muốn so sánh hai cách bằng bằng chứng định lượng, để hiểu trade-off giữa approximate và exact.

#### Acceptance Criteria

1. WHEN so sánh accuracy THEN Report SHALL nêu chênh lệch threshold approximate so với exact cho P90/P80 theo từng nhóm hoặc tổng hợp có thể truy ngược đến nhóm.
2. WHEN so sánh execution time THEN mỗi approach SHALL chạy ít nhất 5 lần và Report SHALL nêu mean cùng standard deviation.
3. WHEN hai approach tạo qualifying-order sets khác nhau trong bất kỳ group nào THEN Report SHALL nhận diện và phân tích các group đó.
4. WHEN hai sets không khác nhau THEN Report SHALL nêu rõ đã kiểm tra và không tìm thấy khác biệt.
5. WHEN benchmark THEN cache, warm-up, input, Spark configuration và điểm bắt đầu/kết thúc timing SHALL được giữ tương đương hoặc ghi rõ để kết quả có thể diễn giải.
6. WHEN đối chiếu full-data THEN P90 có thể lệch threshold ở 43,5% nhóm nhưng chỉ 0,8% lệch SD cuối; P80 lần lượt là 36,7% và 2,4%; Report SHALL phân biệt chênh lệch trung gian với chênh lệch đáp số.

### R-SP-22-05 — Phân tích partition cho group lớn

**User Story:** Là người chấm, tôi muốn phân tích group trên 1,000 orders nếu tồn tại, để đánh giá hiểu biết về partitioning và kích thước dữ liệu.

#### Acceptance Criteria

1. WHEN profiling groups THEN hệ thống SHALL xác định có `(SKU, month)` nào chứa hơn 1,000 orders hay không.
2. IF có group hơn 1,000 orders THEN Report SHALL thảo luận manual repartitioning có lợi hay không cho group đó.
3. IF có group hơn 1,000 orders THEN Report SHALL giải thích partition strategy được chọn.
4. IF có group hơn 1,000 orders THEN Report SHALL liên hệ default partition size điển hình 128 MB với data volume thực của group.
5. IF không có group hơn 1,000 orders THEN Report SHALL nêu kết quả kiểm tra; không bắt buộc tạo một thảo luận giả định như thể điều kiện đã xảy ra.
6. WHEN kiểm tra full-data THEN group lớn nhất có 426 order và khoảng 222 KB; không có group vượt 1.000 order, nên không repartition thủ công cho từng group. Report SHALL ghi vấn đề thực tế là 200 shuffle partitions cho khoảng 69 MB và có thể cân nhắc 8–16 hoặc AQE coalesce.

### R-SP-22-06 — Output và báo cáo Task 2-2

**User Story:** Là người chấm, tôi muốn một Parquet duy nhất và báo cáo đầy đủ, để đọc kết quả bằng Pandas/Spark local và đối chiếu hai approach.

#### Acceptance Criteria

1. WHEN Task 2-2 hoàn tất THEN kết quả SHALL là đúng một tệp vật lý `Task_2-2.parquet` trên filesystem thông thường, đọc được bằng Pandas hoặc Spark local mode.
2. WHEN chọn final result THEN Report/schema SHALL nêu rõ tệp chứa kết quả approach nào hoặc chứa cả hai; mọi trường cần so sánh SHALL có nghĩa không mơ hồ.
3. WHEN viết Report THEN phần Task 2-2 SHALL giải thích cách hiểu, decomposition, implementation strategy, lý do decomposition và toàn bộ so sánh bắt buộc ở R-SP-22-04/R-SP-22-05.
4. WHEN kiểm thử fixture THEN test SHALL bao phủ threshold nằm đúng tại promotion count, duplicate counts quanh percentile, nhóm có 0/1/nhiều qualifying orders và chênh lệch approximate/exact nếu tạo được.

## 8. Báo cáo và cấu trúc bài nộp

### R-SUB-01 — Báo cáo thống nhất

**User Story:** Là người chấm, tôi muốn một `Report.pdf` thống nhất, để đánh giá 0.5 điểm phân tích, 0.5 điểm decomposition và 0.5 điểm reasoning của từng bài.

#### Acceptance Criteria

1. WHEN nộp bài THEN `docs/Report.pdf` SHALL bao phủ riêng từng Task 1-1, 1-2, 2-1 và 2-2.
2. WHEN mô tả mỗi task THEN Report SHALL có cách hiểu query, decomposition thành elemental steps, implementation strategy và lý do của decomposition.
3. WHEN một task có report requirements chuyên biệt THEN Report SHALL bao phủ toàn bộ mục tương ứng trong R-MR-11-04, R-SP-21-04, R-SP-22-04 và R-SP-22-05.
4. WHEN trình bày kết quả THEN Report SHALL chứa hoặc tham chiếu bằng chứng test/export/correctness phù hợp để hỗ trợ các tiêu chí chấm còn lại.

### R-SUB-02 — Cấu trúc thư mục bắt buộc
Revision note 2026-08-12: the final submission tree flattens task source roots to `src/Task_*` direct roots and omits a top-level `scripts/` directory.
Note: if the folder layout changes, any scripts, source path literals, build settings, or docs that point into the old tree must be updated in the same scope so commands still resolve correctly.

**User Story:** Là đại diện nhóm, tôi muốn package đúng cấu trúc, để Moodle chấp nhận và Drive chứa đúng output.

#### Acceptance Criteria

1. WHEN chuẩn bị bài nộp THEN thư mục gốc SHALL tên `<RepresentativeID>` với ID của một thành viên nhóm.
2. WHEN đặt source THEN cấu trúc SHALL có `src/Task_1-1/source`, `src/Task_1-2/source`, `src/Task_2-1/source`, `src/Task_2-2/source` và mã tương ứng trong từng nhánh.
3. WHEN đặt tài liệu THEN cấu trúc SHALL có `docs/Report.pdf`, `docs/drive_link.txt` và `docs/README.md`.
4. WHEN tạo `drive_link.txt` THEN file SHALL chứa một link duy nhất tới Google Drive folder có tên `<RepresentativeID>`.
5. WHEN tổ chức Drive folder THEN nó SHALL chứa trực tiếp đúng bốn output bắt buộc: `Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`, `Task_2-2.parquet`.
6. WHEN nộp Moodle THEN toàn bộ thư mục SHALL được nén thành `<RepresentativeID>.zip` và chỉ một đại diện nhóm nộp.
7. WHEN deadline Moodle đã qua THEN nhóm SHALL NOT sửa Drive content vì đề nêu chỉnh sửa sau deadline làm vô hiệu kết quả.
8. WHEN solution hoàn tất THEN nó SHALL là một lời giải nhóm thống nhất, không phải các lời giải cá nhân tách rời.

## 9. Những điều không được yêu cầu hoặc không được phép

- Không yêu cầu đọc thêm *Spark: The Definitive Guide*; PDF chỉ khuyến nghị.
- Không yêu cầu dùng Spark SQL; direct Spark SQL string query không được chấp nhận cho lời giải Structured APIs, chỉ có thể dùng để minh họa hiểu biết/intermediate reasoning trong báo cáo nếu không trở thành lời giải được chấm.
- Không yêu cầu Python streaming; hơn nữa nếu chọn Python thì streaming-based method cho MapReduce queries bị cấm. Spec chọn Scala nên không xây dựng pipeline Python streaming.
- Không yêu cầu Google Colab và Colab bị cấm.
- Không yêu cầu database, web UI, REST API, cloud deployment hoặc hệ thống realtime.
- Không yêu cầu benchmark mọi bước; chỉ những phép đo được thực hiện/đòi hỏi so sánh mới phải tuân thủ quy tắc tối thiểu 5 lần, mean và standard deviation.
- README là optional theo PDF nhưng trở thành bắt buộc trong spec do yêu cầu trực tiếp của người dùng.
- Nhóm tự chọn schema output phù hợp; spec chỉ yêu cầu schema đủ rõ để kiểm chứng và được khóa ở Design.

## 10. Quyết định đã chốt cho các điểm mập mờ

Các quyết định dưới đây được cập nhật từ `Lab3_Slide_ref.pdf` ngày 2026-08-31. Đây là semantics phải dùng khi triển khai; mọi quyết định không phải câu chữ tuyệt đối của đề đều phải được nhắc lại trong Report.

1. **Task 1-1 bought predicate**: dùng `Status` chứa `shipped` và `Qty > 0`; không dùng `Status = 'Shipped'`, không dùng `Qty != 0`.
2. **Task 1-1 frequency/grain**: một CSV row là một record được đếm; không cộng `Qty` để tạo frequency. `Order ID` lặp không được tự động deduplicate vì mỗi row có thuộc tính SKU/size riêng.
3. **Task 1-1 variance measure**: chọn `Amount`, vì slide khuyến nghị cách này do khớp tên “purchased amount”. `Amount=NULL` vẫn thuộc frequency của bought row nhưng bị loại khỏi moment; nếu không còn Amount hợp lệ thì variance được xem là vô hạn khi phá hòa để ưu tiên nhóm có variance hữu hạn. Quy tắc này phải được ghi trong Report.
4. **Task 1-1 output date**: chọn map-to-buckets `t+1..t+L`, kể cả sau ngày lớn nhất của input; do đó ngày cuối là `29/06 + 10 = 09/07/2022`. Khoảng `[d-L,d-1]` vẫn không bao gồm ngày `d`.
5. **Task 1-2 population**: chọn chỉ dùng bought rows với cùng predicate `Status contains shipped AND Qty > 0` để “purchased” có nghĩa nhất quán.
6. **Task 1-2 scope của “đã từng bán XXL”**: chọn cách toàn cục — style đạt điều kiện nếu từng bán size `>= XXL` ở bất kỳ state/tháng nào — vì file đáp án của giảng viên dùng cách này. Report vẫn phải nêu cách hiểu trong từng `(state, month)` và chênh lệch 40/128 nhóm (31%) để minh bạch.
7. **Task 1-2 median**: số phần tử chẵn lấy trung bình số học của hai giá trị giữa; nhóm không có style qualifying không phát output.
8. **Task 2-1 cancelled**: dùng cách hiểu chính của slide: `Status` chứa `Cancelled` và `ship-service-level = Standard`; các biến thể `Courier = Cancelled` chỉ là kiểm chứng phụ. Tất cả bốn cách hiểu trên slide đều cho 0 đơn đạt `>=3` promotion.
9. **Task 2-1 denominator**: chọn `Cancelled + Standard` orders trong từng `(state, city)` làm mẫu số; numerator là subset đạt thêm `>=3` promotion hợp lệ và `Amount < state average`. Dùng `LEFT JOIN` để order không có promotion vẫn ở mẫu số.
10. **Task 2-2 percentile**: chọn exact nearest-rank `ceil(p*N)`; approximate dùng Spark built-in. Linear interpolation chỉ dùng để giải thích/đối chiếu, không dùng làm kết quả chính.
11. **Task 2-2 standard deviation/null**: dùng population SD (`ddof=0`, `stddev_pop`). Nhóm có dưới 2 Amount hợp lệ hoặc không có Amount hợp lệ xuất `0.0`; không thay null bằng 0 trước khi tính.
12. **Task 2-2 repartition**: không repartition thủ công theo group ở dữ liệu hiện tại vì group lớn nhất chỉ 426 rows/222 KB và không có group >1.000. Chỉ ghi nhận việc giảm `spark.sql.shuffle.partitions` từ 200 xuống khoảng 8–16 hoặc dùng AQE coalesce là hướng tối ưu cần benchmark.
13. **`shapes.parquet(legacy)`**: không thuộc input/logic của PDF; loại khỏi pipeline và submission.

## 11. Thông tin còn cần cung cấp trước khi chạy môi trường thật

Đây là các dữ kiện vận hành, không phải mơ hồ nghiệp vụ. Nếu chưa có, không được giả tạo giá trị:

1. Phiên bản Spark thực tế và mode chạy (`local`, standalone hoặc YARN); phải chạy `spark-submit --version` trước khi khóa dependency.
2. `RepresentativeID`, Drive URL và deadline Moodle trước khi đóng gói cuối.

## 12. Deferred / ngoài phạm vi

| ID | Hạng mục | Lý do |
|---|---|---|
| D1 | Tối ưu cho cluster production nhiều node | Môi trường mục tiêu là Lab 1 local/pseudo-distributed; chỉ phân tích phân tán trong phạm vi đề. |
| D2 | Dashboard/UI xem kết quả | Không có trong đề hoặc grading criteria. |
| D3 | Tự động upload Google Drive hoặc submit Moodle | Là external action của đại diện nhóm; README chỉ hướng dẫn chuẩn bị artefact. |
| D4 | Chuyển đổi dữ liệu nguồn sang database/data lake lâu dài | Không cần cho bốn truy vấn và output bắt buộc. |

## 13. Ma trận truy vết yêu cầu nguồn

| Nguồn trong PDF | Requirement IDs |
|---|---|
| Yêu cầu Scala, môi trường, fallback API | R-GEN-01 |
| Benchmark ≥5, mean, standard deviation | R-GEN-03, R-SP-22-04 |
| Advanced MapReduce problem 1 | R-MR-11-01 đến R-MR-11-04 |
| Advanced MapReduce problem 2 | R-MR-12-01 đến R-MR-12-03 |
| Structured APIs problem 1 | R-SP-21-01 đến R-SP-21-04 |
| Structured APIs problem 2 | R-SP-22-01 đến R-SP-22-06 |
| Report và grading criteria | R-MR-11-04, R-MR-12-03, R-SP-21-04, R-SP-22-04 đến R-SP-22-06, R-SUB-01 |
| Submission structure và exact filenames | R-SUB-02 |
| Code comments/run instructions | R-GEN-01, R-GEN-02 |

## 14. Quality Checklist

### Completeness

- [x] Tất cả vai trò người dùng đã được nhận diện.
- [x] Happy path, edge case và error/null cases đã được nêu ở mức requirement.
- [x] Business rules, giới hạn API/ngôn ngữ/môi trường và output đã được ghi nhận.
- [x] Data/persistence scope, lifecycle, volume và output filenames đã được ghi nhận.
- [x] Report, benchmark, submission structure và grading evidence đã được truy vết.
- [x] Các điểm đề mơ hồ được đưa vào Decision Records, có lựa chọn, lý do và cách triển khai; không tự quyết định âm thầm.
- [x] Revision 2026-08-31 đã được người dùng xác nhận qua Approval Gate.

### Clarity và consistency

- [x] Thuật ngữ state, city, month, order, promotion, variety, window date được dùng nhất quán.
- [x] Các giá trị bắt buộc (`> 10,000`, 5/10 ngày, `>= 2` ngày, `>= 3` promotion, P90/P80, `ddof=0`, `> 1,000`) được giữ đúng theo đề.
- [x] Các điều bị cấm/không yêu cầu được tách riêng.
- [x] Requirement không khóa kiến trúc chưa được duyệt ngoài các kỹ thuật bắt buộc trong đề.

### Testability

- [x] Mỗi requirement có output hoặc hành vi quan sát được.
- [x] Các phép aggregate quan trọng có yêu cầu fixture tính tay.
- [x] Export được xác minh bằng filesystem thường và reader độc lập.
- [x] Benchmark có số lần chạy và thống kê bắt buộc.

## Approval Gate

> Không bắt đầu Detailed Design cho đến khi người dùng xác nhận rõ tài liệu này.

- **Status**: Approved
- **Confirmed by**: Người dùng
- **Confirmation date**: 2026-08-10
- **Revision note (2026-08-12)**: Scope changed to require WSL-friendly README commands, flattened task source roots, and no top-level `scripts/` directory.
- **Notes / required revisions before design**: Người dùng trả lời “approved”. Những điểm chưa có dữ kiện từ giảng viên được Design giải quyết bằng quyết định và giả định công khai, có thể sửa nếu nhận được clarification sau đó.
- **Revision gate**: Người dùng cần xác nhận các quyết định tại Sec 10 trước khi revision này được coi là approved cho code execution.
- **Revision confirmation**: Người dùng xác nhận triển khai revision ngày 2026-08-31.
