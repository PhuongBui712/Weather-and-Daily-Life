FROM apache/airflow:2.6.0-python3.9

RUN mkdir -p /opt/airflow/script

COPY entrypoint.sh entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]