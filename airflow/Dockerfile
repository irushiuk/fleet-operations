FROM apache/airflow:2.9.3-python3.11
USER root
COPY common/ /opt/airflow/common/
USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
