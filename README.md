# Instacart Big Data: phân tích và dự đoán sản phẩm mua lại

Bài tập lớn triển khai quy trình **thu thập → HDFS → Spark SQL → Spark ML → dashboard** trên dữ liệu giao dịch Instacart. Bài toán dự đoán: với một khách hàng và sản phẩm họ đã mua trước đây, ước lượng khả năng sản phẩm đó có mặt trong đơn kế tiếp. Kết quả cuối cùng là 10 sản phẩm gợi ý cho mỗi khách hàng.

Nguồn dữ liệu: [Instacart Market Basket Analysis, Zenodo, DOI 10.5281/zenodo.2654444](https://zenodo.org/records/2654444). Bản ghi mô tả khoảng 3 triệu đơn của hơn 200 nghìn người dùng. Dự án sử dụng **sáu CSV gốc**; không dùng notebook hay chỉ số công bố sẵn trong bản ghi.

**Trạng thái kiểm chứng:** sáu CSV đã tải và khớp MD5; pipeline chạy hoàn tất bằng Spark `local[2]` trên WSL với mẫu khoảng 1% khách hàng thật. PR-AUC = 0,410; Precision@10 = 0,308 so với baseline 0,294. Chưa chạy trên ba VM vì phần triển khai cụm được bàn giao dưới dạng mã và hướng dẫn. Xem số đo và giới hạn trong [báo cáo](docs/REPORT.md).

## Cấu trúc

```text
dashboard/app.py                  Dashboard Streamlit tiếng Việt
src/instacart_bigdata/pipeline.py  PySpark: prepare, quality, analytics, train, score
scripts/download_data.py          Tải dữ liệu + xác minh MD5 theo Zenodo API
scripts/make_fixture.py           Dữ liệu nhỏ để thử toàn bộ quy trình
scripts/run_single.sh             Chạy Spark một máy trên Linux/WSL
scripts/run_cluster.sh            Chạy HDFS + YARN trên ba máy ảo
scripts/check_cluster.sh          Kiểm tra cụm theo cách chỉ đọc
scripts/export_results.sh         Xuất kết quả để xem dashboard
docs/VM_SETUP.md                  Hướng dẫn ba máy ảo VirtualBox
docs/REPORT.md                    Thuyết minh phương pháp và đánh giá
```

## Chạy thử trên một máy Linux

Yêu cầu: Python 3.8–3.12, Java 8/11/17, Spark 3.5.9 (`spark-submit` trong `PATH`). Với Spark cài qua `pip`, dùng:

```bash
python3 -m venv .venv-linux
source .venv-linux/bin/activate
pip install -r requirements.txt
export PYSPARK_PYTHON="$PWD/.venv-linux/bin/python"
bash scripts/run_single.sh fixture    # khoảng vài trăm dòng, không cần Internet
bash scripts/run_single.sh demo       # ~1% khách hàng thật
streamlit run dashboard/app.py
```

Trên Windows, chạy các lệnh Linux trong WSL hoặc trên máy ảo `Master`. Spark ghi Parquet trên Windows thuần cần `winutils.exe`, vì vậy bản chạy một máy ưu tiên Linux/WSL.

## Chạy trên ba máy ảo

Đọc [hướng dẫn cụm VirtualBox](docs/VM_SETUP.md). Sau khi HDFS, YARN và Spark đã hoạt động, copy mã vào `Master`, tải sáu CSV rồi chạy:

Gói mã `bigdata-project.tar.gz` đã được tạo sẵn trong thư mục dự án để copy sang Master; dữ liệu CSV được tải riêng vì kích thước lớn.

```bash
cd ~/Bigdata
python3 scripts/download_data.py --output data/raw  # nếu chưa copy CSV từ Windows
bash scripts/run_cluster.sh demo
bash scripts/export_results.sh demo
```

`demo` giữ xấp xỉ 1% khách hàng (`user_id % 100 == 0`), phù hợp 4 GB RAM trên Master và 2 GB trên mỗi Slave. Chỉ chạy `full` sau khi kiểm tra RAM, dung lượng HDFS và cấu hình YARN. Kết quả nằm trong `outputs/`, mô hình nằm trên HDFS, còn `instacart-results.tar.gz` có thể copy về Windows để xem dashboard.

## Đọc kết quả

- `outputs/quality/`: số lượng bản ghi, khóa trùng, khóa ngoại thiếu và giá trị ngoài phạm vi.
- `outputs/analytics/`: sản phẩm bán nhiều, danh mục, giờ/thứ đặt hàng, kích thước giỏ hàng.
- `outputs/metrics.json`: PR-AUC, ROC-AUC, Precision@10, Recall@10, Hit rate@10 của mô hình và baseline tần suất mua.
- `outputs/predictions/recommendations/`: top 10 sản phẩm cho khách hàng trong tập `test`.

**Lưu ý đánh giá:** tập `train` của Instacart chứa các sản phẩm thật trong đơn kế tiếp. Mỗi cặp khách hàng–sản phẩm từng mua là một ứng viên; cặp không xuất hiện trong đơn `train` nhận nhãn 0. Chia train/validation theo khách hàng, không để lịch sử của cùng người rơi vào cả hai tập. Các đặc trưng chỉ tính từ lịch sử `prior`; cột thời điểm của đơn kế tiếp là thông tin biết khi gợi ý. `Recall@10` đo trên các sản phẩm mua lại thuộc tập ứng viên, không bao gồm sản phẩm khách chưa từng mua. Xem thêm [báo cáo](docs/REPORT.md).

## Tái lập

Lệnh tải sẽ kiểm tra checksum MD5 của sáu CSV theo API Zenodo. Dữ liệu gốc, Parquet và output được `.gitignore` để repo gọn. Nếu muốn chạy lại, xóa hoặc đổi tên thư mục `outputs/` cũ trước khi xuất kết quả mới; script Spark tự ghi đè vùng stage/model/output tương ứng.
