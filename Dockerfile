FROM apache/airflow:2.10.4-python3.11

# Tạm thời chuyển sang người dùng root để cài đặt Java và Git
USER root

# Tải xuống và cài đặt Java thủ công
# Bạn cần tìm đường link tải xuống chính xác cho phiên bản Java bạn muốn
RUN set -eux; \
    export JAVA_HOME="/usr/lib/jvm/default-jvm"; \
    mkdir -p "$JAVA_HOME"; \
    curl -fsSL https://download.oracle.com/java/17/archive/jdk-17.0.12_linux-x64_bin.tar.gz | tar -xzf - -C "$JAVA_HOME" --strip-components=1; \
    export PATH="$JAVA_HOME/bin:$PATH";

# Khai báo JAVA_HOME để các script khác có thể tìm thấy Java
ENV JAVA_HOME="/usr/lib/jvm/default-jvm"

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt