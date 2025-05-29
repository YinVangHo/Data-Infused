FROM apache/airflow:2.10.5

# Install Airflow Snowflake provider
RUN pip install apache-airflow-providers-snowflake

# Install dbt-snowflake
RUN pip install dbt-snowflake

# Install your custom project dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
