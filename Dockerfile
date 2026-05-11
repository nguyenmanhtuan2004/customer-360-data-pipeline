# 1. Sử dụng Python 3.11 Slim (Nhẹ và ổn định)
FROM python:3.11-slim-bullseye

# 2. Thiết lập biến môi trường
# PySpark sẽ tự tìm thấy Spark bên trong thư mục site-packages của nó
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Chỉ cài những gì thực sự cần thiết: Java JRE (để chạy Spark) và procps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 4. Tận dụng Cache cho Requirements
# Bước này sẽ tải pyspark 4.1.1 (đã bao gồm sẵn Spark runtime bên trong)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy mã nguồn
COPY . .

# 6. Port cho Dashboard
EXPOSE 8501

CMD ["python", "main_pipeline.py"]
