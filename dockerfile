FROM apache/airflow:latest

USER root

# Install system dependencies (git in your case)
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean


USER airflow
# Install Python dependencies including Snowflake provider
RUN pip install --no-cache-dir apache-airflow-providers-snowflake==4.0.0
