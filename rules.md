# Big Data Lab 3 — Rules & Ambiguity Decisions

> **Nguồn chính**: [`Lab3_Slide_ref.pdf`](../../../../Lab3_Slide_ref.pdf)
>
> **Phạm vi**: Đây là file quy tắc thực thi cho bốn task. Mọi implementation và Report phải tuân theo các lựa chọn dưới đây. Nếu giảng viên có clarification mới, cập nhật file này trước rồi đồng bộ Goals, Design, Checklist và code.

## 1. Quy tắc chung

### 1.1 Grain và parser

- Một dòng CSV là một `record`/item line và được dùng làm grain chính. `index` là `record_id`.
- Không tự động deduplicate theo `Order ID`; nếu một `Order ID` xuất hiện nhiều dòng, mỗi dòng vẫn có SKU/Size/Amount riêng.
- CSV phải được đọc bằng parser hỗ trợ quote; không dùng `String.split(",")` cho cả dòng vì `promotion-ids` có dấu phẩy bên trong.
- Mỗi record hợp lệ phải có đúng 24 cột.
- Ngày parse strict theo `MM-dd-yy`; tháng dùng dạng `yyyy-MM`.
- Text dimension phải `trim`, collapse whitespace và uppercase bằng `Locale.ROOT` khi dùng làm khóa. Khi output cần giữ giá trị gốc thì chỉ dùng giá trị chuẩn hóa cho grouping, không làm mất giá trị cần báo cáo.

### 1.2 Status, Qty, Size và null

- `Task 1-1 bought`: `Status` chứa `shipped` sau khi chuẩn hóa, không dùng `Status = 'Shipped'`, và `Qty > 0`.
- Các label cố định như `Standard`, `Merchant`, `Shipped` dùng so sánh exact sau chuẩn hóa; chỉ Task 1-1 dùng contains cho `shipped`, và Task 2-1 dùng contains cho `Cancelled` theo rule riêng bên dưới.
- Size không sort alphabet. Dùng rank: `XS=1, S=2, M=3, L=4, XL=5, XXL=6, 3XL=7, ...`.
- `XXL`, `2XL`, `XXXL`, `3XL`, `4XL`, `5XL`, `6XL` được coi là từ XXL trở lên; `Free` và size không nhận diện được thì không đạt.
- `promotion-ids` phải split sau khi CSV đã parse, trim token, bỏ token rỗng và deduplicate token trong cùng một record.
- Không biến `NULL` thành 0 nếu điều đó làm thay đổi ý nghĩa nghiệp vụ. Mọi phép so sánh với null phải có `coalesce`/lọc rõ ràng trong Design và Report.

### 1.3 Môi trường và output

- Lời giải chính dùng Scala trên môi trường Lab 1/pseudo-distributed; không dùng Python/Java thay thế và không dùng Google Colab.
- Bài 1 dùng Hadoop MapReduce thật; Bài 2 dùng Spark DataFrame/Dataset API, không dùng `spark.sql` query string.
- CSV output của Task 1-1/1-2 và Parquet output của Task 2-1/2-2 phải được export thành một file vật lý có đúng tên, không nộp thư mục `part-*`.
- Không dùng `getmerge` để nối Parquet vì có thể làm hỏng footer.
- `shapes.parquet(legacy)` không thuộc input/logic trong slide, không đưa vào pipeline hoặc submission.

## 2. Task 1-1 — Dynamic sliding window MapReduce

### 2.1 Cách hiểu đã chọn

- “Đã bán” là record thỏa `Status contains shipped AND Qty > 0`.
- Frequency là số bought records của từng Size trong cửa sổ, không phải tổng `Qty`.
- Tổng bought records theo state được tính trên toàn dataset.
- State có tổng **lớn hơn 10.000** dùng cửa sổ 5 ngày; state còn lại dùng 10 ngày.
- Baseline từ slide:
  - `MAHARASHTRA`: 19.103 → 5 ngày.
  - `KARNATAKA`: 14.950 → 5 ngày.
  - 44 state còn lại → 10 ngày.

### 2.2 Điểm mơ hồ và quyết định

| Điểm mơ hồ | Cách đã chọn | Lý do |
|---|---|---|
| “Purchased amount” là `Amount` hay `Qty`? | Chọn `Amount` cho variance | Slide khuyến nghị `Amount` vì khớp tên cột |
| Một Order ID lặp tính thế nào? | Tính từng CSV record | Giữ thuộc tính Size/Amount của từng dòng, không tự đặt quy tắc gộp order |
| Ngày `d` có nằm trong cửa sổ không? | Không; cửa sổ là `[d-L, d-1]` | Đúng mô tả “những ngày trước ngày d” |
| Có phát ngày sau ngày cuối input không? | Có, phát `t+1..t+L` | Slide yêu cầu kết quả tới `29/06 + 10 = 09/07/2022` |
| `Amount=NULL` xử lý thế nào? | Vẫn tính frequency; không đưa vào moment variance. Nếu không có Amount hợp lệ, variance là undefined và xếp sau variance hữu hạn | Không để thiếu Amount làm mất bought frequency; vẫn bảo toàn ý nghĩa variance |

### 2.3 Luật phá hòa

Áp dụng theo thứ tự:

1. Frequency lớn hơn thắng.
2. Nếu frequency hòa, population variance của `Amount` nhỏ hơn thắng; variance chia `N`, không chia `N-1`.
3. Nếu vẫn hòa, Size nhỏ hơn theo lexicographical order thắng.

Khi dùng secondary sort theo Size A-Z, reducer phải cập nhật winner bằng `>` thay vì `>=` để giữ Size đến trước khi toàn bộ tiêu chí hòa.

### 2.4 Cách triển khai

1. **Job 0**: đếm bought records theo state, xác định window length và phát bảng state → window length qua Distributed Cache/config.
2. **Mapper**: mỗi bought record ngày `t` phát `(state, t+i, size)` với `i=1..L`, mang accumulator `(count, q, q²)` với `q=Amount` khi Amount hợp lệ.
3. **Combiner/Reducer**: cộng dồn count, `sum(Amount)`, `sum(Amount²)` theo `(state, window_date, size)`.
4. **Winner reducer**: group theo `(state, window_date)`, nhận Size theo secondary sort và áp dụng luật phá hòa.
5. Có thể dùng mảng hiệu để phân tích tối ưu shuffle, nhưng kết quả chính phải tương đương map-to-buckets.

### 2.5 Bằng chứng bắt buộc

- 3.696 dòng output.
- Ngày lớn nhất `2022-07-09`.
- Size `M` thắng 1.299 lần.
- Bảng shuffle phải nêu các mốc slide:
  - Naive: 925.395 phiếu.
  - Mảng hiệu: 219.132 phiếu.
  - Combiner: 27.134 phiếu.
- Report phải có key design, secondary sort, Amount-vs-Qty decision và giải thích off-by-one.

## 3. Task 1-2 — Median variety MapReduce

### 3.1 Cách hiểu đã chọn

- Variety của một style là số SKU **phân biệt** của style trong `(state, month)`.
- Chỉ dùng bought rows với predicate của Task 1-1.
- Style qualifying nếu từng bán size `>= XXL` ở **bất kỳ state/tháng nào trong toàn dataset**.
- Chọn scope toàn cục vì file đáp án của giảng viên dùng cách này.
- Median khi số phần tử chẵn là trung bình số học của hai giá trị giữa.
- Nhóm không có style qualifying không phát output.

### 3.2 Điểm mơ hồ và quyết định

| Điểm mơ hồ | Cách đã chọn | Lý do |
|---|---|---|
| “Style đã từng bán XXL” xét trong nhóm hay toàn cục? | Chọn toàn cục | Khớp file đáp án giảng viên; slide cho biết hai cách lệch 40/128 nhóm (31%) |
| “Goods purchased” có lọc bought không? | Có lọc bought | Nhất quán với semantics purchased đã chốt trong Task 1-1 |
| Size `3XL/XXXL/2XL` xếp thế nào? | Rank từ XXL trở lên đều đạt | Tránh sort alphabet làm `3XL` đứng trước `XXL` sai nghiệp vụ |

Report bắt buộc nêu thêm kết quả/cách hiểu local `(state,month)` để người chấm thấy chênh lệch, nhưng output chính dùng scope toàn cục.

### 3.3 Cách triển khai

1. **Job A1**: group theo `style` trên toàn dataset, tính `max(sizeRank)` và phát tập qualifying styles.
2. **Job A2**: group theo `(state, month, style, sku)`, loại SKU trùng, tính variety theo `(state,month,style)`, rồi lọc bằng tập qualifying từ Job A1.
3. **Job B**: group theo `(state,month)`, sort các variety, tính median exact.

### 3.4 Bằng chứng bắt buộc

- Với output chính theo scope **toàn cục + bought rows** đã chọn: 143 nhóm `(month,state)`; `MAHARASHTRA`, tháng 04/2022 có 863 style và median `3.0`.
- Với phép đối chiếu scope **cục bộ + bought rows**: 128 nhóm; `MAHARASHTRA`, tháng 04/2022 có 621 style và median `4.0`.
- Mốc slide 128 nhóm và median `4.0` thuộc hướng local. Con số 647 style trên slide không tái lập khi đồng thời áp dụng bought predicate đã chọn; phải ghi đây là chênh lệch baseline, không sửa output global để ép khớp.
- Tháng 03 và các mốc chi tiết khác phải được báo riêng cho cả global/local nếu dùng để đối chiếu.
- Report phải chứng minh distinct SKU, rank Size và median chẵn/lẻ.

## 4. Task 2-1 — Cancelled Standard percentage bằng Spark DataFrame

### 4.1 Cách hiểu đã chọn

- Tập ứng viên chính là record có `Status` chứa `Cancelled` và `ship-service-level = Standard`.
- Promotion hợp lệ nếu ngày cuối xuất hiện trừ ngày đầu xuất hiện `>= 2` ngày.
- Tất cả promotion đều được giữ, bao gồm promotion do Amazon phát hành.
- Một record đạt numerator nếu có ít nhất 3 promotion hợp lệ và:

```text
Amount < average Amount của state
```

- State average chỉ tính đơn `Fulfilment=Merchant` và `Courier Status=Shipped`, với Amount hợp lệ.
- Mẫu số là toàn bộ Cancelled+Standard records trong từng `(state, city)`, kể cả record không có promotion.
- Tính phần trăm:

```text
100 × numerator / denominator
```

### 4.2 Điểm mơ hồ và quyết định

| Điểm mơ hồ | Cách đã chọn | Lý do |
|---|---|---|
| “Cancelled” là Status hay Courier Status? | Chính: `Status contains Cancelled`; các cách Courier là kiểm chứng phụ | Đây là cách hiểu chính trong slide |
| Promotion Amazon có được tính? | Có | Slide ghi rõ “including Amazon’s/all promotions” |
| Mẫu số tính trên city hay state? | Group kết quả theo `(state,city)`; mẫu số là Cancelled+Standard của city | Tránh trộn city trùng tên ở state khác và đúng “percentage per city” |
| Order không có promotion join thế nào? | `LEFT JOIN` | Phải giữ order trong mẫu số |

### 4.3 Cách triển khai

1. Group promotion ID toàn dataset để lấy `min(date)`, `max(date)` và lọc `datediff >= 2`.
2. Explode/join promotion hợp lệ về từng record và đếm promotion hợp lệ.
3. Group các Merchant + Courier Shipped records theo state để tạo 40 state thresholds.
4. Tạo denominator từ Cancelled+Standard records, sau đó `LEFT JOIN` promotion count và state average.
5. Tạo numerator bằng điều kiện `valid_promotion_count >= 3` và `Amount < state_average`.
6. Group `(state,city)` và tính percentage.

### 4.4 Kết quả 0% là đúng

- Slide nêu 6.909 Cancelled+Standard records; file CSV hiện tại sau parser/normalization cho 6.906 records và 1.442 `(state,city)` groups.
- Tất cả đều có `promotion-ids` rỗng.
- Khi nới ra 18.332 Cancelled records, chỉ 295 records có promotion và mỗi record chỉ có 1 mã.
- Điều kiện `>= 3` không thể đạt; numerator bằng 0 ở mọi city.
- Bốn cách hiểu Cancelled trên slide đều cho 0 records đạt `>=3` promotion; không được nới điều kiện để tạo số đẹp.

### 4.5 Execution plan bắt buộc

Report phải chạy/đối chiếu hai cấu hình:

| Cấu hình | Join | Exchange | Sort |
|---|---:|---:|---:|
| Mặc định | 3 BroadcastHashJoin | 4 | 0 |
| `autoBroadcastJoinThreshold=-1` | 3 SortMergeJoin | 7 | 6 |

Phải phân biệt kế hoạch tĩnh từ `explain(true)` với executed plan khi AQE bật.

## 5. Task 2-2 — Dynamic P90/P80 bằng Spark DataFrame

### 5.1 Cách hiểu đã chọn

- Grain là một CSV record; group key là `(SKU, calendar month)`.
- Promotion count đếm tất cả promotion tokens trong record, bao gồm Amazon; ô rỗng là 0.
- P90 và P80 được tính riêng trong từng group, không dùng threshold chung.
- Giữ records có `promotion_count >= threshold`.
- Độ lệch chuẩn của Amount dùng population standard deviation: `ddof=0`, `stddev_pop`.
- Nhóm có dưới 2 Amount hợp lệ hoặc không có Amount hợp lệ xuất `0.0`; không thay Amount null bằng 0 trước phép tính.

### 5.2 Điểm mơ hồ và quyết định

| Điểm mơ hồ | Cách đã chọn | Lý do |
|---|---|---|
| Exact percentile dùng interpolation hay rank? | Nearest-rank: `ceil(p*N)` | Cho threshold là promotion count quan sát được và phù hợp ví dụ nearest-rank trên slide |
| Approximate dùng cách nào? | Spark built-in `percentile_approx` qua DataFrame `expr` | Đáp ứng ràng buộc Structured API, không dùng `spark.sql` query |
| Có dùng cùng threshold cho mọi group không? | Không, mỗi `(SKU,month)` có threshold riêng | Đúng từ “dynamic percentile thresholds” |
| Một Order ID lặp có gộp không? | Không, tính từng CSV record | Giữ grain và các thuộc tính item line; phải ghi rõ trong Report |

Linear interpolation chỉ dùng làm phương án so sánh. Không dùng nó cho kết quả exact chính.

### 5.3 Cách triển khai

1. Tạo order-level base với `promotion_count`, SKU, month, Amount và record ID.
2. Group theo `(SKU,month)` để tính P80/P90 approximate bằng built-in.
3. Exact path dùng Window partition `(SKU,month)`, sort `(promotion_count, record_id)`, lấy rank `ceil(p*N)`.
4. Join bảng threshold ngược về từng record trong cùng group.
5. Lọc `promotion_count >= threshold`, group lần cuối và tính `stddev_pop(Amount)`.
6. Xuất cả approximate và exact hoặc ghi rõ method/percentile trong schema để so sánh không mơ hồ.

### 5.4 Bằng chứng bắt buộc

- 16.486 nhóm `(SKU,month)`; promotion count trong khoảng 0–26.
- Với nhóm mẫu `0,0,1,1,2,2,2,3,5,9`, nearest-rank P90 là 5.
- So sánh P90/P80 phải báo threshold difference, qualifying-set difference và final SD difference.
- P90: 43,5% nhóm lệch threshold nhưng 0,8% lệch SD cuối.
- P80: 36,7% nhóm lệch threshold nhưng 2,4% lệch SD cuối.
- Benchmark mỗi method ít nhất 5 lần, báo mean và standard deviation; chạy tương đương về input, cache, warm-up và action.

### 5.5 Repartition

- Có 16.486 nhóm, nhóm lớn nhất 426 records và khoảng 222 KB.
- Không có group nào vượt 1.000 records; không manual repartition theo group.
- 128 MB lớn hơn nhóm lớn nhất khoảng 600 lần.
- Điểm cần benchmark là `spark.sql.shuffle.partitions` mặc định 200 cho khoảng 69 MB; có thể thử 8–16 hoặc AQE coalesce.

## 6. Quy tắc Report và kiểm chứng

Report của mỗi task phải có:

- Cách hiểu câu hỏi và các assumptions/decisions.
- Decomposition thành các bước nhỏ.
- Cách triển khai và lý do chọn.
- Các điểm mơ hồ cùng phương án bị loại/chọn.
- Kết quả kiểm chứng và các số baseline trong file này.

Trước khi nộp:

- Task 1-1: 3.696 dòng CSV, ngày cuối 09/07/2022.
- Task 1-2: 143 dòng CSV cho output global đã chọn; evidence local có 128 dòng.
- Task 2-1: percentage toàn 0% và có chứng minh.
- Task 2-2: đọc lại Parquet bằng Pandas hoặc Spark local, kiểm tra schema và method/percentile key.
- Không sửa production code hoặc đổi decision record nếu chưa cập nhật Goals → Design → Checklist → Notes.

## 7. Trạng thái xác nhận

- Các quyết định trong file này được chọn theo slide và/hoặc file đáp án giảng viên ở nơi đã ghi rõ.
- Spark version/mode thực tế, `RepresentativeID`, Drive URL và deadline vẫn là dữ kiện vận hành cần cung cấp trước full run/package.
- Revision được người dùng xác nhận cho code execution ngày 2026-08-31 qua yêu cầu triển khai, cài môi trường, chạy full-data và đối chiếu slide.
