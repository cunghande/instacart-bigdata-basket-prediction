# Gợi ý thuyết trình 8–10 phút

1. **Đặt vấn đề (45 giây):** một khách hàng quay lại, nên gợi ý sản phẩm nào trong đơn tới? Xác định rõ đây là dự đoán mua lại.
2. **Dữ liệu (60 giây):** nguồn Zenodo, sáu bảng, quan hệ `orders`–`order_products`–`products`, quy mô và checksum.
3. **Kiến trúc (60 giây):** ba VM, HDFS lưu trữ, YARN cấp tài nguyên, Spark SQL xử lý, Spark ML huấn luyện, Streamlit trình bày.
4. **Xử lý dữ liệu (60 giây):** khai báo schema, kiểm tra chất lượng, chuyển CSV sang Parquet, lấy mẫu theo *khách hàng*.
5. **Phân tích (90 giây):** mở dashboard, nêu giờ/thứ cao điểm, sản phẩm/danh mục nổi bật, tỉ lệ mua lại. Dùng số liệu thật của lần chạy.
6. **Dự đoán (90 giây):** tạo cặp khách hàng–sản phẩm từng mua, đặc trưng lịch sử, nhãn từ đơn `train`, chia train/validation theo người.
7. **Đánh giá (90 giây):** trình bày PR-AUC, Precision@10, Recall@10, so với baseline tần suất mua. Giải thích phạm vi chỉ gồm sản phẩm đã mua.
8. **Demo và kết luận (60 giây):** Spark UI/YARN có hai executor, HDFS có hai DataNode, dashboard gợi ý top 10 và giới hạn hiện tại.

## Câu hỏi dễ gặp

**Vì sao là Big Data?** Dữ liệu có hàng triệu đơn và hàng chục triệu dòng sản phẩm; pipeline được thiết kế dùng HDFS, Spark DataFrame/SQL và Spark ML trên hai worker, không gom toàn bộ giao dịch vào pandas trên một máy. Chỉ nói đã chạy phân tán sau khi có minh chứng Spark UI/YARN của phiên trên VM.

**Vì sao không dùng accuracy?** Nhãn 0 nhiều; accuracy có thể cao ngay cả khi mô hình không tìm được sản phẩm mua lại. PR-AUC và top 10 gắn trực tiếp với bài toán gợi ý.

**Có rò rỉ dữ liệu không?** Đặc trưng chỉ lấy từ `prior`; `train` chỉ tạo nhãn. Train/validation chia theo khách hàng.

**Có dự đoán sản phẩm mới không?** Chưa. Ứng viên hiện chỉ là sản phẩm khách từng mua. Có thể thêm candidate generation từ ALS hoặc luật kết hợp.

**Demo 1% có phải toàn bộ dữ liệu không?** Không. Dữ liệu gốc được tải đầy đủ; chế độ demo giữ khoảng 1% người dùng cho phần xử lý và huấn luyện do ba VM chỉ có tổng 8 GB RAM. Báo rõ chế độ chạy trên slide và trong báo cáo.
