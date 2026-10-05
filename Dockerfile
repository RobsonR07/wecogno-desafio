FROM apache/airflow:2.11.0-python3.12
RUN pip install --no-cache-dir apache-airflow==2.11.0 pyarrow==19.0.1 "psycopg[binary]==3.2.6"
