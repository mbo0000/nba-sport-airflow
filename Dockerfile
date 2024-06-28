FROM apache/airflow:2.5.1
COPY requirements.txt .

# Install packages from requirements.txt file
RUN pip install -r requirements.txt

COPY plugins/ /opt/airflow/plugins/


WORKDIR /opt/airflow

