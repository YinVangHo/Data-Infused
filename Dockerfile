#FROM apache/airflow:2.10.5
#RUN pip install apache-airflow-providers-snowflake

FROM apache/airflow:2.10.5

# Optional: install Airflow Snowflake provider
RUN pip install apache-airflow-providers-snowflake

# Install your project dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
