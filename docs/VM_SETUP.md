# Triển khai trên ba máy ảo VirtualBox

## 1. Cấu hình đã kiểm tra trên Windows

| Máy | Hệ điều hành | vCPU | RAM | Ổ ảo | Mạng hiện có |
|---|---|---:|---:|---:|---|
| Master | Ubuntu 20.04 | 2 | 4 GB | 30 GB | NAT Network `NatNetwork`; Host-only `192.168.56.101` (lần chạy trước) |
| Slave01 | Ubuntu 20.04 | 2 | 2 GB | 30 GB | NAT Network `NatNetwork` |
| Slave02 | Ubuntu 20.04 | 2 | 2 GB | 30 GB | NAT Network `NatNetwork` |

Host Windows có 16 GB RAM và ổ D còn khoảng 23 GB tại thời điểm kiểm tra. `NatNetwork` chưa xuất hiện trong danh sách NAT Networks của VirtualBox 7.0.8. Không bật cả ba máy nếu Windows còn ít RAM trống; đóng ứng dụng nặng trước. Dung lượng **bên trong** mỗi Ubuntu cần kiểm tra bằng `df -h`, vì kích thước file VDI trên Windows không cho biết chính xác không gian còn trống trong guest.

## 2. Mạng và dịch vụ cơ bản

Tắt cả ba VM trước khi sửa mạng. Trong VirtualBox, tạo NAT Network tên **`NatNetwork`**, CIDR `10.0.2.0/24`, bật DHCP. Hoặc PowerShell:

```powershell
& 'D:\VirtualBox\VBoxManage.exe' natnetwork add --netname NatNetwork --network '10.0.2.0/24' --enable --dhcp on
& 'D:\VirtualBox\VBoxManage.exe' natnetwork start --netname NatNetwork
```

Sau khi bật từng VM, kiểm tra trong Ubuntu:

```bash
hostname -I
ip route
ping -c 2 10.0.2.1
df -h
free -h
java -version
python3 --version
hdfs version
yarn version
spark-submit --version
```

Trên Master có thể chạy `bash scripts/check_cluster.sh` để in tóm tắt RAM, ổ đĩa, tên máy, HDFS và YARN trước khi chạy pipeline.

Điền IP thực tế của ba VM vào `/etc/hosts` trên **cả ba** theo mẫu (ví dụ, thay bằng IP hiển thị từ `hostname -I`):

```text
10.0.2.195 master
10.0.2.196 slave01
10.0.2.197 slave02
```

Đảm bảo `ping master`, `ping slave01`, `ping slave02` thành công từ cả ba máy. Windows có thể SSH vào `Master` qua adapter Host-only `192.168.56.101` nếu IP này vẫn đúng; các Slave có thể được truy cập qua `Master`.

Master có hai card mạng. Trong `/etc/hosts`, tên `master` phải trỏ đến **IP NAT Network `10.0.2.x`** mà các Slave truy cập được. Script Spark đặt `spark.driver.host=master` và lắng nghe trên các interface để executor kết nối đúng vào driver.

## 3. HDFS/YARN/Spark

Nếu các VM đã có Hadoop và Spark từ môn học, giữ cấu hình hiện tại. Xác minh:

```bash
start-dfs.sh                # chạy trên Master nếu HDFS chưa khởi động
start-yarn.sh               # chạy trên Master nếu YARN chưa khởi động
jps                         # Master: NameNode, ResourceManager; Slave: DataNode, NodeManager
hdfs dfsadmin -report        # thấy cả 2 DataNode
yarn node -list              # thấy cả 2 NodeManager
hdfs dfs -mkdir -p /user/$USER/test
hdfs dfs -ls /user/$USER
spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi \
  "$SPARK_HOME/examples/jars/spark-examples_2.12-3.5.9.jar" 2
```

Nếu thiếu Spark, cài **cùng phiên bản 3.5.9** trên Master và các Slave từ [Apache Spark Downloads](https://spark.apache.org/downloads.html), chọn bản dựng Hadoop 3. Spark 3.5 yêu cầu Java 8/11/17 và Python 3.8+; xem [hướng dẫn chính thức](https://spark.apache.org/docs/3.5.9/api/python/getting_started/install.html). Đặt `SPARK_HOME`, thêm `$SPARK_HOME/bin` vào `PATH`, cùng một đường dẫn Python trên các máy. Nếu Hadoop/YARN chưa có, hoàn tất cấu hình cụm theo bài thực hành của lớp trước khi chạy pipeline. **Không format NameNode đang chứa dữ liệu.**

Cài thư viện Python trên **cả ba VM** (trong cùng môi trường Python mà Spark dùng):

```bash
python3 -m pip install --user 'numpy<3' 'pandas<3' setuptools
export PYSPARK_PYTHON=python3
```

Nếu `python3 -m pip` chưa có, cài `python3-pip` trước. Dashboard chỉ cần chạy trên Windows, không cần cài Streamlit trên hai Slave.

Với mỗi Slave 2 GB RAM, cấu hình YARN nên cho phép ít nhất 1 GB/container. `run_cluster.sh` dùng driver 1 GB trên Master và hai executor, mỗi executor 512 MB heap + 384 MB overhead và một core. Kiểm tra `yarn.scheduler.maximum-allocation-mb` và `yarn.nodemanager.resource.memory-mb` trước khi chạy. Nếu cụm không đủ RAM, dùng `run_single.sh demo` trên Master trước. Chế độ `full` có thể cần tăng RAM executor và VM.

Để hai executor nằm trên **hai Slave khác nhau**, mỗi Slave có thể khai báo `yarn.nodemanager.resource.memory-mb=1536` và `yarn.scheduler.minimum-allocation-mb=256` trong cấu hình YARN, rồi khởi động lại NodeManager/ResourceManager. Container 896 MB sẽ được cấp 1.024 MB, nên một Slave 1.536 MB chỉ chứa một executor. Xác nhận vị trí thật bằng tab **Executors** của Spark UI; không suy ra từ số lượng executor đơn thuần.

Script đặt replication HDFS = 1 cho dữ liệu của dự án để tiết kiệm dung lượng trên ba ổ ảo 30 GB. Cấu hình này giảm khả năng chịu lỗi; nếu `hdfs dfs -df -h /` cho thấy đủ chỗ, tăng replication lên 2 sau khi chạy demo.

## 4. Chuyển mã và dữ liệu

Từ Windows PowerShell, thay `<ubuntu-user>` bằng tài khoản của bạn. Tạo gói chỉ chứa mã và tài liệu, không chứa `.venv` hay Parquet:

```powershell
cd D:\Code\Bigdata
tar -czf bigdata-project.tar.gz --exclude=__pycache__ src scripts dashboard docs README.md requirements.txt
scp bigdata-project.tar.gz <ubuntu-user>@192.168.56.101:~/
```

Trên Master, giải nén và tải trực tiếp sáu CSV (khoảng 713 MB):

```bash
mkdir -p ~/Bigdata && cd ~/Bigdata
tar -xzf ~/bigdata-project.tar.gz
python3 scripts/download_data.py --output data/raw
```

Lệnh tải xác minh sáu MD5. Không copy `.venv/` Windows sang Ubuntu. Sau đó:

```bash
bash scripts/run_cluster.sh demo
bash scripts/export_results.sh demo
```

Nếu `demo` thành công và `hdfs dfs -df -h /` còn đủ dung lượng, có thể chạy `bash scripts/run_cluster.sh full`. Dữ liệu full cần nhiều shuffle và HDFS hơn; demo là đường chạy ổn định hơn cho ba VM hiện có.

Để mở dashboard trên Windows, copy `instacart-results.tar.gz` từ Master về, giải nén vào thư mục dự án, rồi chạy Streamlit bằng Python Windows:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install streamlit pandas
scp <ubuntu-user>@192.168.56.101:~/Bigdata/instacart-results.tar.gz .
tar -xzf instacart-results.tar.gz
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

## 5. Minh chứng nên chụp khi nộp bài

1. `hdfs dfsadmin -report`: hai DataNode hoạt động.
2. `yarn node -list`: hai NodeManager hoạt động.
3. Spark UI trong bước train: executors, stages, job chạy phân tán.
4. `hdfs dfs -ls -R /user/$USER/instacart`: raw, stage Parquet, output và model.
5. Dashboard: biểu đồ phân tích, so sánh baseline, top 10 sản phẩm.

Lưu ảnh minh chứng cùng phiên chạy thật; không dùng số liệu của fixture để trình bày như kết quả trên bộ dữ liệu gốc.

Giao diện thường dùng trên mạng VM: NameNode `http://master:9870`, YARN `http://master:8088`, Spark UI `http://master:4040` khi job đang chạy. Cổng thực tế phụ thuộc cấu hình Hadoop/Spark hiện có.
