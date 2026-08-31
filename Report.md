# Big Data Lab 3 — Báo cáo phân tích và triển khai

> **Trạng thái**: Report draft  
> **Dataset**: `Amazon Sale Report.csv`  
> **Công nghệ**: Scala 2.11.12, Hadoop MapReduce 3.3.6, Spark 2.4.8, SBT 1.5.8  
> **Mã nguồn**: [`23127442`](23127442/)

## 1. Giới thiệu

### 1.1. Mục tiêu

Lab 3 yêu cầu xử lý dataset Amazon Sale bằng Hadoop MapReduce và Spark Structured APIs. Mục tiêu không chỉ là tạo bốn output, mà còn phải giải thích cách dữ liệu được chuẩn hóa, shuffle giữa các bước, lựa chọn physical execution plan và sự khác nhau giữa percentile gần đúng/chính xác.

Các mục tiêu cụ thể:

1. Đọc CSV đúng chuẩn, kể cả quoted field có dấu phẩy.
2. Chuẩn hóa status, state, city, style, SKU và size trước khi group/join.
3. Giữ grain một CSV row = một record/item line.
4. Triển khai hai pipeline Hadoop MapReduce có intermediate output và combiner/reducer phù hợp.
5. Triển khai hai pipeline Spark bằng DataFrame transformations, không dùng SQL query string làm lời giải chính.
6. Phân tích join, shuffle, sort, Window, benchmark và khả năng đọc lại output.

**Ghi chú ảnh — Hình 1 (phần 1.1)**: Chèn sơ đồ tổng quan `Dataset → Parser/Normalization → 4 Tasks → Validation → 4 Outputs`. Đây là hình nên có để người đọc hiểu pipeline ngay từ đầu; không cần ảnh chụp terminal.

### 1.2. Tổng quan bốn task

| Task | Kỹ thuật | Bài toán | Output |
|---|---|---|---|
| 1-1 | Hadoop MapReduce | Size thắng trong dynamic sliding window theo state/ngày | `Task_1-1.csv` |
| 1-2 | Hadoop MapReduce | Median variety của style theo state/tháng | `Task_1-2.csv` |
| 2-1 | Spark DataFrame | Tỷ lệ đơn Cancelled + Standard đạt điều kiện promotion/amount | `Task_2-1.parquet` |
| 2-2 | Spark DataFrame | So sánh percentile promotion approximate và exact | `Task_2-2.parquet` |

### 1.3. Quy trình thực hiện

Quy trình gồm: kiểm tra dataset → xác định điểm mơ hồ → parse và chuẩn hóa → triển khai từng task → kiểm thử logic → benchmark/plan evidence → export single-file → đọc lại bằng validator → đối chiếu slide → đóng gói.

Build Scala đã thành công và bốn output đã qua validator. Tuy nhiên Windows hiện tại không có WSL/Docker và Hadoop yêu cầu `winutils.exe`; full pseudo-distributed Hadoop và Spark write cần được xác nhận lại trên Lab 1/WSL.

## 2. Chuẩn bị và tiền xử lý dữ liệu

### 2.1. Dataset và schema

Dataset có 24 cột. Các cột chính:

- `index` → `record_id`;
- `Date` → ngày, parse strict `MM-dd-yy`, tạo `month=yyyy-MM`;
- `Status`, `Fulfilment`, `Courier Status`, `ship-service-level`;
- `Style`, `SKU`, `Size`;
- `Qty`, `Amount`;
- `ship-city`, `ship-state`;
- `promotion-ids`, danh sách promotion có thể có nhiều token.

Parser dùng Commons CSV, không dùng `String.split(",")` cho cả dòng vì `promotion-ids` có thể chứa dấu phẩy nằm trong quote.

**Ghi chú ảnh — Hình 2 (phần 2.1)**: Chèn bảng rút gọn header 24 cột và một dòng có nhiều promotion token. Có thể dùng ảnh CSV hoặc bảng Markdown; không cần ảnh terminal dài.

### 2.2. Các vấn đề dữ liệu

1. Status có nhiều biến thể như `Shipped - Delivered to Buyer`, `Shipped - Returned to Seller`, `Pending`, `Cancelled`.
2. Qty có giá trị 0; bought không được hiểu là `Qty != 0` mà là `Qty > 0`.
3. Text có whitespace, chữ hoa/thường và giá trị rỗng không đồng nhất.
4. Promotion cần split sau khi CSV đã parse, trim token rỗng và deduplicate trong từng record.
5. Amount null không được âm thầm đổi thành 0 trong variance/stddev.
6. Order ID có thể lặp; không deduplicate theo Order ID.
7. Size `XXL`, `2XL`, `XXXL`, `3XL`,… phải so theo rank, không sort alphabet.
8. City trùng tên giữa các state nên khóa phải là `(state, city)`.

### 2.3. Giả định và cách xử lý điểm mơ hồ

Các lựa chọn được ghi đầy đủ trong [`rules.md`](rules.md).

| Điểm mơ hồ | Cách chọn | Lý do |
|---|---|---|
| Bought | `Status contains shipped AND Qty > 0` | Đúng semantics đã chốt |
| Grain | Mỗi CSV row là một record | Giữ thuộc tính item line |
| Task 1-1 variance | Dùng `Amount`, population variance | Theo khuyến nghị slide |
| Task 1-1 window | Emit `t+1..t+L` | Tương ứng cửa sổ `[d-L,d-1]` |
| Task 1-2 scope | Global qualifying style | Khớp file đáp án giảng viên |
| Task 2-1 cancelled | `Status contains Cancelled` + Standard | Theo rule chính |
| Task 2-1 join | `LEFT JOIN` | Giữ record không có promotion |
| Task 2-2 exact | Nearest-rank `ceil(p*N)` | Threshold là giá trị quan sát được |
| Null statistic | Không đổi null thành 0 trước tính toán | Bảo toàn semantics |

### 2.4. Tiền xử lý

1. Xác nhận header và 24 cột.
2. Parse ID, date, numeric và optional text.
3. `trim`, collapse whitespace và uppercase bằng `Locale.ROOT` cho các dimension.
4. Tạo `month`.
5. Tạo predicate:

```text
bought = contains(upper(trim(Status)), "SHIPPED") AND Qty > 0
```

6. Parse promotion token sau CSV parser.
7. Giữ null để từng task áp dụng chính sách riêng.

### 2.5. Cấu trúc mã nguồn tiền xử lý và mã nguồn dùng chung

Mã nguồn được chia thành ba nhóm. Các file thực thi riêng của Task 1-1, Task 1-2, Task 2-1 và Task 2-2 được trình bày ở mục 3–6 tương ứng, nên không liệt kê lại trong bảng dưới đây. Phần này chỉ mô tả các file dùng để đọc/chuẩn hóa dữ liệu và các tiện ích dùng chung cho pipeline, xuất kết quả, kiểm thử và thu thập evidence.

```text
23127442/src/common/source/lab3/
├── common/   # mô hình dữ liệu, CSV parser, chuẩn hóa và CLI
├── spark/    # reader/transform dùng chung cho các Spark task và plan evidence
└── io/       # export output và validation
```

#### 2.5.1. Mã nguồn tiền xử lý dữ liệu

| File | Công dụng | Vai trò trong pipeline |
|---|---|---|
| [`SaleRow.scala`](23127442/src/common/source/lab3/common/SaleRow.scala) | Định nghĩa `SaleRow`, mô hình dữ liệu chuẩn ở row grain và hàm suy ra `month` từ ngày. | Là cấu trúc trung gian cho nhánh Hadoop và giúp thống nhất ý nghĩa một dòng CSV là một record/item line. |
| [`SaleRowParser.scala`](23127442/src/common/source/lab3/common/SaleRowParser.scala) | Parse một CSV record bằng Apache Commons CSV; kiểm tra header 24 cột, kiểu số, ngày `MM-dd-yy` và các trường bắt buộc. | Đọc dữ liệu an toàn khi field có dấu phẩy nằm trong dấu ngoặc kép; trả về `Either[DataError, SaleRow]` để tách dòng lỗi khỏi dòng hợp lệ. |
| [`Normalization.scala`](23127442/src/common/source/lab3/common/Normalization.scala) | Trim, gom whitespace, uppercase bằng `Locale.ROOT`; xác định `isBought`, nhận diện size từ XXL trở lên và tách/deduplicate promotion token. | Đảm bảo các task dùng cùng một bought predicate (`Status` chứa `SHIPPED` và `Qty > 0`), cùng cách so sánh dimension và cùng cách đếm promotion. |
| [`CsvEncoding.scala`](23127442/src/common/source/lab3/common/CsvEncoding.scala) | Escape giá trị khi tạo CSV output, bao gồm comma, quote và line break. | Bảo đảm các output CSV của Hadoop có header/field hợp lệ và đọc lại được bằng CSV parser. |
| [`DataError.scala`](23127442/src/common/source/lab3/common/DataError.scala) | Khai báo các lỗi structural, missing field và invalid field, kèm tên cột và thông báo. | Chuẩn hóa cách báo lỗi parse để dữ liệu sai có thể được đếm/log mà không làm sai semantics của dữ liệu hợp lệ. |
| [`SparkSaleReader.scala`](23127442/src/common/source/lab3/spark/SparkSaleReader.scala) | Khai báo explicit raw schema, đọc CSV ở permissive mode, cast kiểu, chuẩn hóa cột và tạo `month`, `is_bought`. | Là điểm vào chung của Task 2-1 và Task 2-2; tránh để mỗi Spark task tự định nghĩa schema/normalization khác nhau. |
| [`PromotionFrames.scala`](23127442/src/common/source/lab3/spark/PromotionFrames.scala) | Explode promotion IDs, trim/deduplicate token, tính lifespan theo promotion và lọc promotion có span tối thiểu. | Cung cấp các DataFrame trung gian cho Task 2-1 và Task 2-2, đặc biệt là temporal validity và promotion count theo record. |

#### 2.5.2. Mã nguồn dùng chung và hỗ trợ kiểm thử

| File | Công dụng | Khi nào được dùng |
|---|---|---|
| [`Cli.scala`](23127442/src/common/source/lab3/common/Cli.scala) | Parse option dạng `--name value` và flag dạng `--overwrite`; phát hiện option thiếu/trùng/sai. | Được các `Main` dùng để kiểm tra input path, output path, work path và evidence directory trước khi chạy. |
| [`SingleFileExporter.scala`](23127442/src/common/source/lab3/io/SingleFileExporter.scala) | Gom part CSV của Hadoop thành một CSV có header; lấy một part Parquet và di chuyển thành file output đơn; kiểm tra schema read-back và dọn thư mục tạm. | Dùng ở bước export cuối để người dùng nhận đúng bốn file output thay vì thư mục part-file của framework. |
| [`OutputValidator.scala`](23127442/src/common/source/lab3/io/OutputValidator.scala) | Kiểm tra schema, header, logical key không trùng, kiểu dữ liệu, range và các invariant của bốn output. | Xác nhận output sau khi chạy task và phát hiện lỗi định dạng/logic cơ bản trước khi nộp. |
| [`ValidationMain.scala`](23127442/src/common/source/lab3/io/ValidationMain.scala) | Điều phối `OutputValidator` cho hai CSV và hai Parquet, sau đó in trạng thái validation tổng hợp. | Chạy độc lập sau pipeline, không tham gia tính toán task; kết quả `[OK]` được ghi vào report/evidence. |
| [`PlanEvidence.scala`](23127442/src/common/source/lab3/spark/PlanEvidence.scala) | Ghi extended logical/optimized/physical plan và đếm node `Exchange`, `Sort`, join strategy. | Dùng trong Task 2-1 để so sánh broadcast với no-broadcast và lưu raw plan vào `docs/evidence/task21`. |
| [`StageCollector.scala`](23127442/src/common/source/lab3/spark/StageCollector.scala) | Spark listener theo dõi các stage thuộc một job group đã chọn. | Hỗ trợ thu thập runtime/stage evidence cho benchmark và phân tích execution, không thay đổi kết quả dữ liệu. |

Như vậy, lớp tiền xử lý chịu trách nhiệm biến CSV thô thành dữ liệu có schema, kiểu và semantics thống nhất; lớp dùng chung chịu trách nhiệm nhận tham số, export an toàn, validation và ghi evidence. Các task chỉ sử dụng các lớp này rồi tập trung vào logic MapReduce/Spark riêng được phân tích ở các phần sau.

## 3. Task 1-1 — Dynamic Sliding Window MapReduce

### 3.1. Yêu cầu và cách hiểu

Với mỗi state và ngày `d`, chọn size thắng trong các ngày trước đó. Window length phụ thuộc tổng bought records của state:

```text
L(state) = 5, nếu tổng bought records > 10.000
           10, nếu ngược lại
```

Frequency là số bought records, không phải tổng Qty. Baseline slide nêu Maharashtra và Karnataka dùng 5 ngày; các state còn lại dùng 10 ngày.

### 3.2. Công thức và luật thắng

Record ngày `t` được phát tới:

```text
(state, t+1, size), ..., (state, t+L, size)
```

Accumulator của một size gồm:

```text
count       = số record
amountCount = số Amount khác null
sum         = Σ Amount
sumSquares  = Σ Amount²
```

Population variance:

```text
variance = sumSquares / amountCount - (sum / amountCount)²
```

Comparator:

1. Frequency lớn hơn thắng.
2. Nếu hòa, variance nhỏ hơn thắng.
3. Nếu tiếp tục hòa, size có lexical order nhỏ hơn thắng.

### 3.3. Decomposition và mã nguồn

**Job 0 — State count**: Mapper emit `(state,1)`, combiner/reducer cộng count, driver chọn window length và truyền state → length qua Hadoop configuration.

**Job A — Bucket aggregation**: Mapper phát mỗi bought row vào `L` ngày tương lai; combiner cộng `Moment`; reducer cộng theo `(state, window_date, size)`.

**Job B — Winner**: Mapper đọc candidates, reducer group theo `(state, window_date)` và áp dụng comparator; driver export single CSV.

Source: [`Task11Main.scala`](23127442/src/Task_1-1/Task11Main.scala), [`BoughtCountJob.scala`](23127442/src/Task_1-1/BoughtCountJob.scala), [`BucketJob.scala`](23127442/src/Task_1-1/BucketJob.scala), [`Winner.scala`](23127442/src/Task_1-1/Winner.scala), [`Moment.scala`](23127442/src/Task_1-1/Moment.scala).

### 3.4. Input/output contract và pseudocode

Input của cả ba job là cùng một CSV. Intermediate output chỉ dùng nội bộ; output cuối có schema:

```text
state,window_date,window_days,winning_size,frequency,population_variance
```

Pseudocode của driver:

```text
counts = countBoughtByState(input)
windows = { state -> (5 if counts[state] > 10000 else 10) }
buckets = mapEachBoughtRowToFutureDates(input, windows)
candidates = aggregateMomentByStateDateSize(buckets)
winners = chooseWinnerByStateDate(candidates, frequency, variance, size)
exportSingleCsv(winners)
```

Một record có `Amount=NULL` vẫn tạo một `Moment(count=1, amountCount=0)`. Khi combine, `count` vẫn tăng còn `sum/sumSquares` không tăng. Nhờ vậy frequency không bị phụ thuộc vào việc Amount có bị thiếu hay không.

### 3.5. Lập luận tính đúng đắn

- Job 0 đếm từng bought record đúng một lần theo state, nên window length được xác định từ toàn bộ input.
- Với mỗi record ngày `t`, mapper phát đúng các ngày `t+1` tới `t+L`; do đó khi reducer xử lý ngày `d`, nó nhận đúng các record có ngày nguồn thuộc `[d-L,d-1]`.
- `Moment.combine` là phép cộng các thành phần `(count, amountCount, sum, sumSquares)`, nên associative và có thể dùng an toàn ở combiner cũng như reducer.
- Winner reducer so sánh tuần tự với cùng comparator frequency → variance → lexical size, nên kết quả không phụ thuộc thứ tự candidate đầu vào.

### 3.6. Input lỗi và edge cases

- Header chỉ được bỏ qua ở offset 0.
- CSV record sai cấu trúc hoặc numeric/date sai được đưa vào rejected counter, không làm hỏng toàn bộ accumulator hợp lệ.
- State/size thiếu bị loại khỏi bucket vì không thể tạo khóa group.
- Nếu mọi Amount của một candidate đều null, variance undefined và candidate đó đứng sau variance hữu hạn khi frequency hòa.
- Nếu ngày nguồn nằm sát cuối dataset, các bucket tương lai vẫn được phát; đây là lý do output có ngày `2022-07-09`.

### 3.7. Độ phức tạp và shuffle

Với `R` bought records, window `L` và số bucket `B`:

- Job 0: `O(R)`.
- Job A phát tối đa `L` record/bought row: `O(RL)` intermediate emissions.
- Combiner giảm dữ liệu truyền nhưng không đổi worst-case.
- Job B phụ thuộc số candidate trong từng state/date.

Slide nêu các mốc shuffle cần ghi trong Report: naive 925.395 phiếu, difference-array 219.132 phiếu và combiner 27.134 phiếu. Lần chạy Windows chưa lấy được Hadoop counters vì thiếu `winutils.exe`.

### 3.8. Kết quả và kiểm thử

- `Task_1-1.csv` có 3.696 dòng.
- Ngày lớn nhất là `2022-07-09`.
- Size `M` là winner nhiều nhất, xuất hiện 1.299 lần theo baseline slide.
- Independent recomputation có cùng keys/winner/frequency; sai khác variance tối đa khoảng `2.91e-11` do thứ tự tính số thực.
- Amount null vẫn được tính frequency nhưng không vào variance.

**Ghi chú ảnh — Hình 3 (phần 3.3)**: Chèn sơ đồ Job 0 → Job A → Job B.

**Ghi chú ảnh — Hình 4 (phần 3.2)**: Chèn timeline minh họa record ngày `t` được phát vào `t+1..t+L`, làm rõ off-by-one.

**Ghi chú ảnh — Hình 5 (phần 3.8, tùy chọn)**: Chèn hai đoạn nhỏ đầu/cuối output, trong đó đoạn cuối thể hiện ngày `2022-07-09`.

## 4. Task 1-2 — Median Variety MapReduce

### 4.1. Yêu cầu và cách hiểu

Variety của style trong `(state, month)` là số SKU distinct. Chỉ dùng bought rows.

Điểm mơ hồ là scope của “style từng bán size ≥ XXL”:

- **Local**: xét trong từng `(state, month)`.
- **Global**: chỉ cần style từng có size ≥ XXL ở bất kỳ state/tháng nào.

Bài làm chọn **global** vì khớp file đáp án giảng viên. Vì vậy style qualifying ở một nơi vẫn được tính variety tại state/month khác nếu có bought rows.

### 4.2. Công thức

```text
QualifyingStyles = { style | tồn tại bought row có sizeRank >= rank(XXL) }
variety(state, month, style) = count(distinct SKU)
```

Median trên các variety đã sort:

- `n` lẻ: lấy phần tử giữa.
- `n` chẵn: lấy trung bình hai phần tử giữa.

### 4.3. Decomposition và mã nguồn

**Job A1**: lọc bought row có size ≥ XXL, emit style, reducer loại trùng và tạo global style set.

**Job A2**: lọc bought rows có state/style/SKU; giữ style thuộc global set; group `(state,month,style)` và đếm distinct SKU.

**Job B**: group variety theo `(state,month)`, sort và tính median exact.

Global set được truyền sang Job A2 bằng Base64-safe Hadoop configuration vì tập style nhỏ.

Source: [`GlobalStyleJob.scala`](23127442/src/Task_1-2/GlobalStyleJob.scala), [`VarietyJob.scala`](23127442/src/Task_1-2/VarietyJob.scala), [`MedianJob.scala`](23127442/src/Task_1-2/MedianJob.scala), [`Task12Main.scala`](23127442/src/Task_1-2/Task12Main.scala).

### 4.4. Input/output contract, pseudocode và tính đúng đắn

Input của Job A1 là toàn bộ CSV ở row grain. Job A1 tạo một tập `qualifyingStyles`; tập này là dữ liệu điều khiển cho Job A2 chứ không phải một kết quả cuối cần nộp. Output cuối của Task 1-2 có schema:

```text
state,month,median_variety,qualifying_style_count
```

Pseudocode:

```text
qualifyingStyles = {}
for row in input:
    if bought(row) and sizeRank(row.size) >= sizeRank("XXL"):
        qualifyingStyles.add(row.style)

varieties = {}
for row in input:
    if bought(row) and row.style in qualifyingStyles:
        varieties[(row.state, row.month, row.style)].add(row.sku)

for (state, month) in groupByStateMonth(varieties):
    values = sort(distinctSkuCount for each style)
    emit(state, month, median(values), len(values))
```

Lập luận tính đúng đắn:

- A1 xét toàn bộ input nên một style được đánh dấu qualifying nếu và chỉ nếu tồn tại ít nhất một bought row có size ≥ XXL ở bất kỳ state/tháng nào; đây chính là cách hiểu global đã chọn.
- A2 dùng tập SKU thay vì đếm số dòng, nên một SKU lặp lại trong nhiều item line không làm tăng variety.
- Khóa `(state, month, style)` giữ các style tách biệt trước khi Job B gom theo state/tháng; vì vậy median được tính trên đúng một giá trị variety cho mỗi style.
- Job B sort các giá trị và áp dụng quy tắc median lẻ/chẵn, nên kết quả xác định và không phụ thuộc thứ tự mapper/reducer.

Các edge case cần nêu trong report:

- Style, SKU, state hoặc month thiếu bị loại khỏi phép đếm vì không thể tạo khóa hoàn chỉnh.
- Hai record có cùng `(state, month, style, SKU)` chỉ đóng góp một SKU.
- Nếu không có qualifying style tại một state/tháng thì không phát sinh output cho group đó.
- Với số style chẵn, median là trung bình của hai giá trị giữa; không làm tròn về số nguyên.
- Kích thước `2XL`, `XXL`, `3XL` được so bằng rank size đã chuẩn hóa, không so chuỗi alphabet.

### 4.5. Độ phức tạp

- Job A1: `O(R)` mapper và shuffle theo style.
- Job A2: gần `O(R)`; reducer giữ tập SKU distinct của từng group.
- Job B: group có `k` variety values cần `O(k log k)` để sort.
- Bộ nhớ phụ thuộc group lớn nhất và số SKU distinct của group.

### 4.6. Kết quả và so sánh scope

Theo global:

- Output có 143 state-month groups.
- Maharashtra `2022-04`: median `3.0`, 863 qualifying styles.
- Output: [`Task_1-2.csv`](23127442/outputs/Task_1-2.csv).

Đối chiếu local:

- Có 128 groups.
- Maharashtra `2022-04`: median `4.0`, 621 qualifying styles khi áp dụng bought predicate.
- Chi tiết: [`task12-global-vs-local.csv`](23127442/docs/evidence/independent-validation/task12-global-vs-local.csv).

Do 128 là mốc của local scope còn file đáp án dùng global, output chính không được sửa để ép về 128. Con số 647 style trên slide cũng không tái lập khi đồng thời áp dụng bought predicate đã chọn; local recomputation cho 621 style và median 4.0. Report cần ghi đây là baseline không tái lập được từ bộ rule hiện tại.

**Ghi chú ảnh — Hình 6 (phần 4.3)**: Chèn sơ đồ A1 → global style set → A2 → Job B.

**Ghi chú ảnh — Hình 7 (phần 4.6)**: Chèn bảng Global/Local với số group, số style và median của Maharashtra. Đây là hình quan trọng nhất của Task 1-2 vì giải thích trực tiếp chênh lệch 143 và 128.

**Ghi chú ảnh — Hình 8 (phần 4.3, tùy chọn)**: Chèn một ví dụ key/value intermediate, không chụp toàn bộ Hadoop output.

## 5. Task 2-1 — Cancelled Standard Percentage bằng Spark

### 5.1. Yêu cầu và công thức

Denominator là record thỏa:

```text
Status contains CANCELLED
AND ship-service-level = STANDARD
AND state, city không null
```

Promotion `p` hợp lệ nếu:

```text
datediff(maxDate(p), minDate(p)) >= 2
```

Record qualifying nếu:

```text
valid_promotion_count >= 3
AND Amount < state_average_amount
```

State average chỉ tính `Fulfilment=MERCHANT`, `Courier Status=SHIPPED`, Amount khác null. Promotion count và state average được `LEFT JOIN` vào denominator để giữ record không có promotion.

```text
percentage = 100 × qualifying_orders / cancelled_standard_orders
```

### 5.2. Decomposition và mã nguồn

1. `SparkSaleReader` đọc explicit schema và chuẩn hóa cột.
2. `PromotionFrames.tokens` explode, trim và deduplicate token.
3. Group promotion để lấy min/max date và lọc span ≥ 2.
4. Đếm valid promotion theo record.
5. Tính state average.
6. Tạo denominator, left join các bảng phụ.
7. Tạo numerator và group `(state,city)`.
8. Export một Parquet vật lý.

Source: [`Task21Job.scala`](23127442/src/Task_2-1/Task21Job.scala), [`Task21Main.scala`](23127442/src/Task_2-1/Task21Main.scala), [`PromotionFrames.scala`](23127442/src/common/source/lab3/spark/PromotionFrames.scala).

### 5.3. Input/output contract, pseudocode và tính đúng đắn

Task 2-1 đọc toàn bộ dataset ở row grain và xuất một Parquet với schema:

```text
state,city,cancelled_standard_orders,qualifying_orders,percentage
```

Pseudocode của pipeline:

```text
base = readCsvWithExplicitSchema(input).normalize()
validPromotions = {
    promotion_id | datediff(max(order_date), min(order_date)) >= 2
}
promotionCounts = countDistinct(validPromotions by record_id)
stateAverage = avg(amount by state
                   where fulfilment = MERCHANT
                   and courier_status = SHIPPED
                   and amount is not null)

denominator = rows where status contains CANCELLED
              and service_level = STANDARD
              and state/city are not null
enriched = denominator
           LEFT JOIN promotionCounts by record_id
           LEFT JOIN stateAverage by state
qualifying = valid_promotion_count >= 3
             and amount is not null
             and amount < state_average_amount
result = group enriched by (state, city)
         count all rows, sum qualifying rows
         percentage = 100 * qualifying / count
```

Lập luận tính đúng đắn:

- `PromotionFrames.temporallyValidTokens` gom theo promotion ID và chỉ giữ token có khoảng ngày từ min đến max ít nhất hai ngày; một token lặp trong cùng record chỉ được tính một lần.
- `LEFT JOIN` bảo toàn mọi record trong denominator. Record không có promotion hợp lệ nhận count 0 nhờ `coalesce`, nên không bị loại khỏi mẫu số.
- State average chỉ dùng tập MERCHANT/SHIPPED có Amount hợp lệ. Điều kiện `amount < average` không thể cho kết quả true khi Amount hoặc average bị null, phù hợp với chính sách null đã chọn.
- Group theo `(state, city)` tạo một tỷ lệ độc lập cho từng địa điểm; không gộp các thành phố trùng tên giữa các state.
- Vì `cancelled_standard_orders` là số dòng denominator và `qualifying_orders` là số dòng thỏa điều kiện trong chính tập đó, phép chia tạo ra phần trăm đúng theo định nghĩa.

Các edge case và quyết định cần ghi rõ:

- Status được kiểm tra theo `contains("CANCELLED")`, nên cả biến thể có hậu tố/chú thích chứa từ này đều được xem là Cancelled.
- Promotion cùng ngày có span bằng 0 không hợp lệ; span đúng 2 ngày được giữ.
- Record không có promotion hợp lệ vẫn ở denominator nhưng có `valid_promotion_count = 0`.
- Amount null không được biến thành 0 và không thể là qualifying order.
- State average null làm mọi record của state đó không qualifying, nhưng không làm mất record khỏi denominator.
- Ba dòng thiếu state/city được loại theo contract hiện tại; đây là nguyên nhân một phần khiến mẫu số khác baseline trên slide.

### 5.4. Physical execution plan

Hai cấu hình cần phân tích:

| Cấu hình | Join mục tiêu | Exchange mục tiêu | Sort mục tiêu |
|---|---:|---:|---:|
| Mặc định | 3 `BroadcastHashJoin` | 4 | 0 |
| Tắt broadcast | 3 `SortMergeJoin` | 7 | 6 |

BroadcastHashJoin phù hợp khi một phía nhỏ được broadcast. Khi đặt `autoBroadcastJoinThreshold=-1`, Spark phải repartition và sort hai phía để dùng SortMergeJoin, nên Exchange/Sort tăng.

`explain(true)` gồm logical, analyzed, optimized và physical plan. Executed plan sau action mới phản ánh plan thật khi AQE/configuration được áp dụng.

Evidence: [`task21/extended-plan.txt`](23127442/docs/evidence/task21/extended-plan.txt) và [`task21/README.md`](23127442/docs/evidence/task21/README.md).

### 5.5. Kết quả và kiểm thử

- 1.442 state-city groups.
- Denominator 6.906 records.
- Numerator 0; percentage 0% ở mọi group.
- Có 18.332 record chứa Cancelled; 295 record có promotion nhưng mỗi record chỉ một mã, không đạt ≥3.

Slide nêu baseline 6.909/1.435. Hai số này không tái lập từ CSV hiện tại sau parser/normalization, nhưng kết luận 0% khớp và được giữ nguyên trong Report.

Phân tích nguyên nhân cho thấy hai cách đếm khác nhau:

| Cách đếm | Candidate rows | Group rows | Đặc điểm |
|---|---:|---:|---|
| Implementation hiện tại | 6.906 | 1.442 | Loại 3 dòng thiếu state/city, group theo `(state, city)` |
| Slide-like | 6.909 | 1.435 | Giữ cả candidate thiếu location và có dấu hiệu group theo `city` |

Trong dataset thô, 6.909 là tổng số dòng Cancelled + Standard trước khi loại 3 dòng thiếu location. Con số 1.435 cũng trùng với distinct city khi giữ giá trị city rỗng. Đây là bằng chứng mạnh cho thấy slide có thể đang dùng mẫu số trước lọc location và khóa city-only, dù slide không mô tả đủ hai quy tắc này. Implementation giữ `(state, city)` và location hợp lệ để tránh gộp hai city trùng tên ở các state khác nhau. Cả hai cách đều cho percentage `0%`.

**Ghi chú ảnh — Hình 9 (phần 5.4)**: Chèn bảng join strategy, Exchange và Sort giữa cấu hình mặc định/tắt broadcast.

**Ghi chú ảnh — Hình 10 (phần 5.4, tùy chọn)**: Chèn ảnh cắt physical plan có `BroadcastHashJoin`; raw plan đầy đủ giữ trong `docs/evidence/task21/extended-plan.txt`.

**Ghi chú ảnh — Hình 11 (phần 5.2)**: Chèn sơ đồ `valid promotions → record counts → state average → denominator → final group`.

## 6. Task 2-2 — Dynamic P80/P90 bằng Spark

### 6.1. Yêu cầu và công thức

Group key là `(SKU, month)`. Mỗi record có promotion count sau khi split/deduplicate token; token rỗng có count 0.

Với group có `N` records và percentile `p`:

```text
rank = ceil(p × N)
exact_threshold = phần tử rank sau khi sort promotion_count tăng dần
```

Giữ record có `promotion_count >= threshold`, rồi tính:

```text
stddev_pop = sqrt(Σ(x - mean)² / n)
```

Amount null không tham gia amount count/stddev. Nếu qualifying set có dưới 2 record hoặc không có Amount hợp lệ, output là `0.0`.

### 6.2. Decomposition và mã nguồn

1. Tạo `skuMonthBase` ở row grain.
2. Đếm promotion theo record.
3. Approximate dùng Spark `percentile_approx`.
4. Exact dùng Window partition, `row_number` và `ceil(p*N)`.
5. Join threshold về record trong cùng group.
6. Lọc qualifying và tính `stddev_pop`.
7. Ghi approximate/exact cùng Parquet, có `method` và `percentile_level`.

Source: [`Task22Pipeline.scala`](23127442/src/Task_2-2/Task22Pipeline.scala), [`Task22Job.scala`](23127442/src/Task_2-2/Task22Job.scala), [`Task22Main.scala`](23127442/src/Task_2-2/Task22Main.scala), [`BenchmarkHarness.scala`](23127442/src/Task_2-2/BenchmarkHarness.scala).

### 6.3. Input/output contract, pseudocode và tính đúng đắn

Output Parquet có tám cột:

```text
sku,month,method,percentile_level,threshold,
qualifying_order_count,amount_value_count,amount_stddev_pop
```

Mỗi group `(SKU, month)` có bốn dòng: `approx/P80`, `approx/P90`, `exact/P80` và `exact/P90`.

Pseudocode:

```text
base = for each valid row:
       (record_id, sku, month, amount, promotion_count)

approxThresholds = percentile_approx(
    promotion_count grouped by (sku, month),
    [0.8, 0.9], accuracy
)

exactThresholds = for each (sku, month, p):
    ordered = sort promotion_count asc, record_id asc
    rank = ceil(p * group_size)
    threshold = ordered[rank]

for threshold in approxThresholds ∪ exactThresholds:
    qualifying = rows in same group where promotion_count >= threshold
    emit count(qualifying), count(non-null amount), stddev_pop(amount)
```

Lập luận tính đúng đắn:

- `record_id` giữ row grain và làm khóa ổn định khi promotion count được join trở lại; không có bước deduplicate Order ID.
- Approximate dùng đúng API `percentile_approx` với accuracy được truyền vào. Exact dùng Window trong từng `(sku, month)` và chọn phần tử có `row_number = ceil(p*N)`, đúng nearest-rank đã chốt.
- `record_id` là tie-breaker bổ sung để thứ tự Window xác định; tie-breaker không thay đổi threshold khi các promotion count bằng nhau.
- Điều kiện `promotion_count >= threshold` được áp dụng riêng cho từng method/percentile, sau đó `stddev_pop` chỉ tính trên Amount không null. Vì vậy `amount_value_count` phản ánh đúng số giá trị được dùng cho độ lệch chuẩn.
- Khi qualifying set có ít hơn hai dòng hoặc Amount đều null, output `0.0` theo contract thay vì để Spark trả null.

Các edge case cần trình bày:

- Group có một record có rank P80/P90 bằng 1; record đó là threshold và được giữ nếu count ≥ threshold.
- Promotion field rỗng hoặc chỉ có whitespace tạo count 0; token trùng trong cùng record không làm tăng count.
- Các record có promotion count bằng threshold đều được giữ, không chỉ giữ riêng record tại vị trí percentile.
- Amount null không làm mất record khỏi qualifying order count nhưng bị loại khỏi amount value count/stddev.
- Approximate threshold có thể bằng hoặc khác exact threshold; cần so sánh cả threshold và qualifying set, không chỉ so sánh stddev cuối.

### 6.4. Độ phức tạp và repartition

Với `N` records và group size `k`:

- Base/promotion count: gần `O(N)`.
- Approximate aggregate: gần `O(N)` theo aggregation structure.
- Exact Window: khoảng `O(Σ k log k)` vì phải sort trong group.
- Final statistics: `O(N)`.

Dataset có 16.486 groups, group lớn nhất 426 rows, không có group trên 1.000 rows. Vì vậy không manual repartition theo group. Có thể benchmark `spark.sql.shuffle.partitions` từ 200 xuống 8–16 hoặc AQE coalesce nếu chạy target runtime.

### 6.5. Benchmark và kết quả

Benchmark phải warm-up, chạy ít nhất 5 lần/method, dùng cùng cached input/action và báo mean + sample standard deviation.

| Method | Runs | Mean (ms) | Stddev (ms) | Cách tính |
|---|---:|---:|---:|---|
| Approximate | 5 | 782.8 | 19.2666 | `percentile_approx` |
| Exact | 5 | 866.6 | 25.7934 | nearest-rank + Window |

Evidence cần dùng: `benchmark-samples.csv`, `benchmark-summary.csv`, `threshold-deltas`, `set-difference-summary`, `set-difference-examples`, `group-profile.txt` và `extended-plan.txt`. Xem hướng dẫn tại [`task22/README.md`](23127442/docs/evidence/task22/README.md).

Kết quả full-data:

- 128.975 valid rows.
- 16.486 SKU-month groups.
- Group lớn nhất 426 rows; group >1.000 là 0.
- Output có 65.944 rows = 4 dòng/group: approximate/exact × P80/P90.
- Đối soát hiện tại không ghi nhận threshold difference.

Slide có nêu các tỷ lệ group bị lệch threshold giữa các cách tính percentile. Kết quả hiện tại không lặp lại các tỷ lệ đó vì implementation này chốt exact theo nearest-rank `ceil(p*N)` và approximate theo Spark `percentile_approx`; hơn nữa dataset/runtime đang dùng cho đối soát không tạo threshold delta. Vì vậy không được ghi rằng hai kết quả “giống slide tuyệt đối”; cần ghi rõ định nghĩa percentile, accuracy và dataset khi so sánh. Nếu chạy target runtime cho ra delta khác, phải thay số trong bảng bằng evidence target và giải thích ảnh hưởng tới qualifying set/stddev.

| Nội dung cần báo cáo | Kết quả hiện tại | Nguồn |
|---|---:|---|
| Valid rows | 128.975 | `group-profile.txt`/đối soát độc lập |
| SKU-month groups | 16.486 | `group-profile.txt` |
| Maximum group | 426 rows | `group-profile.txt` |
| Groups > 1.000 | 0 | `group-profile.txt` |
| Approx/exact threshold delta | 0 groups | `threshold-deltas` |
| Approx mean/stddev | 782.8 / 19.2666 ms | `benchmark-summary.csv` |
| Exact mean/stddev | 866.6 / 25.7934 ms | `benchmark-summary.csv` |

**Ghi chú ảnh — Hình 12 (phần 6.5)**: Chèn biểu đồ cột mean runtime approximate/exact, lấy từ `benchmark-summary.csv`.

**Ghi chú ảnh — Hình 13 (phần 6.4)**: Chèn bảng group profile và ví dụ `0,0,1,1,2,2,2,3,5,9` có P90 nearest-rank bằng 5.

**Ghi chú ảnh — Hình 14 (phần 6.5, tùy chọn)**: Chèn một ví dụ từ `set-difference-examples`; không chèn toàn bộ các CSV comparison.

## 7. Kiểm thử, validation và artefacts

### 7.1. Build và validation

Lệnh build:

```text
sbt clean test assembly
```

Build/assembly thành công. Submission tree tối giản không chứa test source nên test phase không phát hiện test case trong cây submission.

Lệnh validator:

```text
ValidationMain --output-dir 23127442/outputs
```

Kết quả:

```text
[OK] Bốn output đã qua schema, key và invariant validation
```

### 7.2. Bốn output cuối

| File | Nội dung |
|---|---|
| [`Task_1-1.csv`](23127442/outputs/Task_1-1.csv) | Winner size theo state/window date |
| [`Task_1-2.csv`](23127442/outputs/Task_1-2.csv) | Median variety theo state/month |
| [`Task_2-1.parquet`](23127442/outputs/Task_2-1.parquet) | Cancelled Standard percentage |
| [`Task_2-2.parquet`](23127442/outputs/Task_2-2.parquet) | Approx/exact P80/P90 statistics |

### 7.3. Evidence

[`docs/evidence`](23127442/docs/evidence) lưu raw execution plan, benchmark, group profile, threshold/set comparison và independent validation. Các file này hỗ trợ Report; Report mới là nơi giải thích và kết luận.

### 7.4. Đối chiếu slide

| Nội dung | Kết quả hiện tại | Đánh giá |
|---|---:|---|
| Task 1-1 rows | 3.696 | Khớp |
| Task 1-1 last date | 2022-07-09 | Khớp |
| Task 1-2 global rows | 143 | Cách đã chọn theo file đáp án |
| Task 1-2 local rows | 128 | Mốc local trên slide |
| Task 2-1 percentage | 0% | Khớp |
| Task 2-1 denominator/groups | 6.906 / 1.442 | Khác slide 6.909 / 1.435 |
| Task 2-2 group count | 16.486 | Khớp |
| Task 2-2 maximum group | 426 | Khớp |
| Task 2-2 groups > 1.000 | 0 | Khớp |

**Ghi chú ảnh — Hình 15 (phần 7.1)**: Chèn ảnh hoặc đoạn text ngắn của kết quả validator `[OK]`.

**Ghi chú ảnh — Hình 16 (phần 7.2)**: Chèn bảng cây bốn output với đúng tên file.

**Ghi chú ảnh — Hình 17 (phần 7.4)**: Chèn bảng slide/current; đây là bảng quan trọng hơn ảnh terminal dài.

## 8. Kết luận

Task 1-1 cho thấy MapReduce phù hợp với việc phát record vào nhiều window và dùng combiner/accumulator để giảm shuffle. Task 1-2 cho thấy việc chốt scope global/local có thể thay đổi trực tiếp số group và median; bài làm chọn global theo file đáp án và giữ local comparison để audit.

Task 2-1 cho thấy join strategy phụ thuộc kích thước bảng và broadcast threshold; kết quả 0% phải được giải thích bằng dữ liệu promotion chứ không được xem là lỗi. Task 2-2 cho thấy approximate có chi phí thấp hơn về lý thuyết, còn exact dùng Window/sort và có rank xác định; group profile giúp quyết định repartition có cần thiết hay không.

Bài học chính là phải chốt semantics trước khi code, giữ nhất quán grain từ parser tới output, phân biệt logical với executed plan và lưu evidence đủ để tái kiểm tra. Trước khi nộp chính thức, cần chạy lại trên Lab 1/WSL để lấy Hadoop counters và fresh Spark executed plan/benchmark target; sau đó đóng gói Report cùng README, drive link và bốn output.
