FROM apache/airflow:3.4.0-python3.13
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim texlive-full git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Here we will mount the data volume
RUN mkdir -p /qa4sm/data

USER airflow

# GENERAL requirements not assigned to any task image
COPY base_requirements.txt /base_requirements.txt

# Requirements for AIRFLOW (NOT the individual DAGs, which have their own image)
# Packages are pinned in base_requirements.txt, the constraints pin all other
# dependencies of airflow to the versions tested by the Airflow project.
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.13.txt
RUN pip install --no-cache-dir -r /base_requirements.txt --constraint "${AIRFLOW_CONSTRAINTS}"

USER airflow
