"""Streamlit dashboard for small CSV exports produced by the Spark pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Instacart | Big Data", page_icon="🛒", layout="wide")
st.title("🛒 Instacart: phân tích & dự đoán giỏ hàng")
st.caption("Dữ liệu giao dịch Instacart · Xử lý phân tán với Apache Spark")
st.caption("Thẻ tổng quan đếm mọi đơn trong mẫu; các biểu đồ hành vi dùng đơn lịch sử (prior).")

default_output = Path(__file__).resolve().parents[1] / "outputs"
output_root = Path(st.sidebar.text_input("Thư mục kết quả", str(default_output)))
analytics_root = output_root / "analytics"


@st.cache_data(show_spinner=False)
def load_table(root: str, name: str) -> pd.DataFrame:
    parts = sorted((Path(root) / name).glob("part-*.csv"))
    if not parts:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(part) for part in parts), ignore_index=True)


def table(name: str) -> pd.DataFrame:
    return load_table(str(analytics_root), name)


overview = table("overview")
if overview.empty:
    st.info("Chưa có kết quả. Chạy các bước prepare, quality, analytics và train trong README.md.")
    st.stop()

values = dict(zip(overview["metric"], overview["value"]))
cols = st.columns(4)
for column, (key, label) in zip(cols, [
    ("users", "Khách hàng"), ("orders", "Đơn hàng"),
    ("prior_order_items", "Dòng sản phẩm"), ("products", "Sản phẩm"),
]):
    column.metric(label, f"{int(values.get(key, 0)):,}")

tab1, tab2, tab3 = st.tabs(["Tổng quan", "Sản phẩm & danh mục", "Dự đoán"])

with tab1:
    a, b = st.columns(2)
    by_hour = table("orders_by_hour")
    by_dow = table("orders_by_dow")
    if not by_hour.empty:
        with a:
            st.subheader("Đơn hàng theo giờ")
            st.bar_chart(by_hour.set_index("order_hour_of_day")["orders"])
    if not by_dow.empty:
        with b:
            st.subheader("Đơn hàng theo thứ")
            st.caption("Mã thứ 0–6 trong dữ liệu; nguồn không cung cấp ánh xạ ngày cụ thể.")
            st.bar_chart(by_dow.set_index("order_dow")["orders"])
    basket = table("basket_size")
    if not basket.empty:
        st.subheader("Kích thước giỏ hàng")
        st.bar_chart(basket[basket["basket_size"] <= 40].set_index("basket_size")["orders"])

with tab2:
    top = table("top_products")
    departments = table("departments")
    if not top.empty:
        st.subheader("20 sản phẩm được mua nhiều nhất")
        st.bar_chart(top.set_index("product_name")["purchase_count"])
        st.dataframe(top, hide_index=True, width="stretch")
    if not departments.empty:
        st.subheader("Danh mục")
        display_departments = departments.assign(reorder_rate=departments["reorder_rate"] * 100)
        st.dataframe(display_departments, hide_index=True, width="stretch",
                     column_config={"reorder_rate": st.column_config.ProgressColumn(
                         "Tỷ lệ mua lại", min_value=0, max_value=100, format="%.1f%%")})
        st.caption("Tỷ lệ mua lại = số dòng có reordered = 1 / tổng dòng sản phẩm của danh mục.")

with tab3:
    metrics_path = output_root / "metrics.json"
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        st.subheader("Đánh giá mô hình trên khách hàng chưa gặp trong huấn luyện")
        st.caption(f"Nguồn: {metrics.get('stage', 'không rõ')} · Spark master: {metrics.get('execution_master', 'không rõ')}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("PR-AUC", f"{metrics['area_under_pr']:.3f}")
        m2.metric("ROC-AUC", f"{metrics['area_under_roc']:.3f}")
        m3.metric("Tỷ lệ mẫu dương", f"{metrics['positive_rate']:.1%}")
        if "repeat_item_coverage" in metrics:
            m4.metric("Sản phẩm có thể gợi ý", f"{metrics['repeat_item_coverage']:.1%}")
        comparison = pd.DataFrame([
            {"Phương pháp": "Hồi quy logistic", **metrics["model_top10"]},
            {"Phương pháp": "Baseline: tần suất mua", **metrics["frequency_baseline_top10"]},
        ])
        st.dataframe(comparison, hide_index=True, width="stretch",
                     column_config={
                         "precision_at_10": st.column_config.NumberColumn("Precision@10", format="%.3f"),
                         "recall_at_10": st.column_config.NumberColumn("Recall@10", format="%.3f"),
                         "hit_rate_at_10": st.column_config.NumberColumn("Hit rate@10", format="%.3f"),
                         "users": "Khách hàng",
                     })
        st.caption("Ứng viên chỉ gồm sản phẩm từng mua. Recall@10 tính trên các sản phẩm mua lại trong tập ứng viên.")
    else:
        st.info("Chưa có metrics.json. Chạy bước train để đánh giá mô hình.")

    recommendations = load_table(str(output_root / "predictions"), "recommendations")
    if not recommendations.empty:
        st.subheader("Gợi ý 10 sản phẩm cho một khách hàng trong tập test")
        user = st.selectbox("Mã khách hàng", recommendations["user_id"].unique().tolist())
        selected = recommendations[recommendations["user_id"] == user]
        st.dataframe(selected, hide_index=True, width="stretch",
                     column_config={"score": st.column_config.NumberColumn("Xác suất", format="%.3f")})

quality = load_table(str(output_root / "quality"), "quality")
with st.expander("Kiểm tra chất lượng dữ liệu"):
    if quality.empty:
        st.write("Chưa chạy bước quality.")
    else:
        st.dataframe(quality, hide_index=True, width="stretch")
