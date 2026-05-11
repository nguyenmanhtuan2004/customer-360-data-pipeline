import streamlit as st
import streamlit.components.v1 as components

# Cấu hình trang mở rộng toàn màn hình
st.set_page_config(page_title="Marketing Dashboard", layout="wide")

st.title("📊 Customer 360 Marketing Dashboard")
st.markdown("Báo cáo dưới đây được nhúng trực tiếp từ Power BI Service.")

# Dán cái đường link Power BI bạn vừa copy vào đây
POWER_BI_URL = "https://app.powerbi.com/view?r=eyJrIjoiZmY2NTJkOTgtOWM3Yi00MmQ4LWJiZmUtNjM4YjcwOWQxZWMwIiwidCI6Ijk4YWRhNjgwLWUzZjQtNDhjYi04ZmJiLWM4YjEwY2I5N2FlZCIsImMiOjEwfQ%3D%3D"

# Nhúng Power BI vào Streamlit
components.iframe(POWER_BI_URL, width=1200, height=800, scrolling=True)
