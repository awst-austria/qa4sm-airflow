# make sure that the versions here match with the environment.yml and base_requirements.txt
FROM apache/airflow:2.9.2-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Here we will mount the data volume
RUN mkdir -p /qa4sm/data

USER airflow

# GENERAL requirements not assigned to any task image
COPY base_requirements.txt /base_requirements.txt

# Requirements for AIRFLOW (NOT the individual DAGs, which have their own image)
RUN pip install --no-cache-dir -r /base_requirements.txt

USER airflow