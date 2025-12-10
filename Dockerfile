FROM apache/airflow:3.1.3
USER root

RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
        default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /
RUN /home/airflow/.local/bin/python -m pip install --upgrade pip setuptools wheel
RUN /home/airflow/.local/bin/python -m pip install -r /requirements.txt
USER airflow