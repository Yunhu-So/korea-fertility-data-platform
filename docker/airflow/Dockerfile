FROM apache/airflow:2.9.3-python3.11

# Airflow 공식 이미지 권장: pip는 airflow 유저로 실행
USER airflow

COPY requirements.txt /tmp/requirements.txt
COPY requirements-dev.txt /tmp/requirements-dev.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt -r /tmp/requirements-dev.txt
