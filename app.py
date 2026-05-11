import streamlit as st

# 1. Cấu hình trang mở rộng toàn màn hình
st.set_page_config(page_title="Marketing Dashboard", layout="wide")

st.title("📊 Customer 360 Marketing Dashboard")
st.markdown("Báo cáo dưới đây được nhúng trực tiếp từ Power BI Service.")

# 2. Link Power BI của bạn
# Lưu ý: Khi bạn sửa tiêu đề trong Power BI, có thể mất 15-60 phút để link công khai này cập nhật theo.
POWER_BI_URL = "https://app.powerbi.com/view?r=eyJrIjoiZmY2NTJkOTgtOWM3Yi00MmQ4LWJiZmUtNjM4YjcwOWQxZWMwIiwidCI6Ijk4YWRhNjgwLWUzZjQtNDhjYi04ZmJiLWM4YjEwY2I5N2FlZCIsImMiOjEwfQ%3D%3D"

# 3. Nhúng Power BI (Dùng st.iframe bản mới nhất)
st.iframe(POWER_BI_URL, height=800)
