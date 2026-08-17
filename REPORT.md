# Báo cáo LAB 17 - Data Pipeline Engineering

**Họ tên:** Trần Đức Mạnh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

## 0. Kết quả kiểm tra

Kết quả cuối sau ba lượt chạy: `gold_training_set` 12.480 dòng, `gold_feature_daily` 9.100 dòng, `gold_doc_chunks` 31.200 dòng và `quarantine_tickets` 312 dòng. Checksum của cả bốn bảng không đổi giữa ba lượt; `dbt test` đạt 11/11, `priority` chỉ còn 1..4 và không có NULL. Lỗi thiếu `data/gold_events/*.parquet` ở cuối thuộc bài mở rộng chưa seed, không ảnh hưởng ba nhiệm vụ chính.

| Bảng | Checksum lượt 1 | Lượt 2 | Lượt 3 |
|---|---:|---:|---:|
| `gold_training_set` | `8dd7c98653` | `8dd7c98653` | `8dd7c98653` |
| `gold_feature_daily` | `3db448685c` | `3db448685c` | `3db448685c` |
| `gold_doc_chunks` | `92d8e50131` | `92d8e50131` | `92d8e50131` |
| `quarantine_tickets` | `ebb89036fb` | `ebb89036fb` | `ebb89036fb` |

## 1. Bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Chạy lại pipeline làm `gold_training_set` tăng dòng và lặp `ticket_id`, dù grain là một hàng cho một ticket. |
| **Nguyên nhân** | Model incremental không có `unique_key` và chiến lược ghi nên dbt chèn thêm. Khi retry cùng partition, dòng cũ không được nhận diện để cập nhật. |
| **Cách khắc phục** | Khai báo `unique_key='ticket_id'`, dùng `merge`; đồng thời đặt DAG `catchup=False`, `max_active_runs=1` để tránh chạy bù và các DAG run chồng nhau. |
| **Bằng chứng** | Trước sửa, bảng có lúc lên 26.270 dòng nhưng chỉ có 12.480 ticket. Sau sửa còn đúng 12.480 dòng, không lặp; checksum ba lượt cuối đều là `8dd7c98653`. |

## 2. Bảng đặc trưng theo ngày thiếu dữ liệu quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng ổn định nhưng chỉ có 8.645 dòng, thiếu 455 dòng so với kỳ vọng 9.100. |
| **P99 độ trễ đo được** | **2,7258 ngày**. P50 là 0,1281 ngày, P95 là 1,8137 ngày, độ trễ lớn nhất là 2,9447 ngày; khoảng 5,05% sự kiện đến muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày**, bằng cách làm tròn P99 lên một ngày đầy đủ. |
| **Nguyên nhân** | Điều kiện chỉ lấy `event_date` lớn hơn ngày lớn nhất trong đích. Ví dụ event ngày 12/08 tới kho ngày 15/08 sẽ không lọt qua điều kiện và bị bỏ quên. |
| **Cách khắc phục** | Tính lại cửa sổ ba ngày; dùng khóa ghép `(event_date, customer_id)` và `merge` để lần tính sau thay thế dòng cũ. |
| **Bằng chứng** | Số dòng tăng từ 8.645 lên đúng 9.100; `gold_training_set` vẫn giữ 12.480 dòng nên thay đổi không làm hỏng Nhiệm vụ 1. |

Tôi chọn P99 vì `max` dễ bị kéo lệch bởi một vài outlier. Window càng rộng thì mọi lượt chạy sau đều phải đọc và tính lại nhiều dữ liệu hơn. Với bộ dữ liệu này, làm tròn P99 lên ba ngày cũng bao phủ giá trị max 2,9447 ngày. Trường hợp trễ hơn nên được theo dõi và backfill riêng.

## 3. Kiểu dữ liệu `priority` thay đổi

| | |
|---|---|
| **Triệu chứng** | Silver có 6.488 `priority` NULL, còn xuất hiện -1, 0, 5; trong khi `quarantine_tickets` rỗng và 9 test cũ vẫn pass. |
| **Nguyên nhân** | Source đổi một phần từ số sang nhãn chữ. `try_cast` biến nhãn hợp lệ như `urgent` thành NULL nhưng vẫn nhận 0, 5, -1 vì chúng là số nguyên. |
| **Ba nhóm giá trị** | Các chuỗi số `1..4` được giữ nguyên; `urgent/high/medium/low` được map lần lượt về `1/2/3/4`; các giá trị `P1`, `P2`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng và NULL được xem là lỗi và đưa vào quarantine. |
| **Cách khắc phục** | Viết lại macro bằng `CASE`; lọc dòng lỗi trước `row_number`; dùng cùng macro cho quarantine. Bật contract và thêm test `not_null`, `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` có đúng 312 dòng; `priority` trong Silver chỉ còn 1..4 và không NULL; `dbt test` tăng từ 9 lên 11 test và đạt 11/11. Bảng Gold vẫn cho đúng 12.480 training rows. |

Tôi giữ dữ liệu thô ở Bronze để còn bằng chứng điều tra, còn chuẩn hóa và chặn lỗi ở Silver. Không nên để 312 dòng lỗi làm dừng toàn bộ dữ liệu hợp lệ; chúng được tách vào quarantine để xử lý sau, còn contract và test ngăn lỗi đi tiếp xuống Gold.

## 4. Tổng kết

| Nhiệm vụ | Điều tôi sẽ kiểm tra trước khi sửa một pipeline chưa quen |
|---|---|
| 1 | Xác định grain, khóa duy nhất và hành vi khi job bị retry. |
| 2 | So sánh event time với ingestion time, sau đó đo percentile trước khi chọn lookback. |
| 3 | Phân biệt schema evolution với dữ liệu hỏng, đồng thời kiểm tra contract, test và đường đi của bản ghi bị từ chối. |
