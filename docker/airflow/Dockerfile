FROM apache/airflow:2.9.3-python3.11

# According to the official Airflow image, pip should run as the airflow user
USER airflow

COPY requirements.txt /tmp/requirements.txt
COPY requirements-dev.txt /tmp/requirements-dev.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt -r /tmp/requirements-dev.txt
