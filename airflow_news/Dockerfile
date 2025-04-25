FROM apache/airflow:2.10.5-python3.11 as base

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip 

USER airflow
USER root

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc wget

# Install astro cli
RUN apt-get update && apt-get install -y curl unzip && \
    curl -sSL https://install.astronomer.io | bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.astro:${PATH}"


EXPOSE 8080
EXPOSE 5000


USER airflow
RUN pip install numpy


RUN pip install -r /requirements.txt --no-cache-dir
RUN pip install newspaper4k
USER root

USER airflow

