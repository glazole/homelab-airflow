# Keep core Airflow version and Python version fixed.
FROM apache/airflow:3.1.8-python3.12

USER root
# The current Outline/Keycloak stack uses a locally issued root CA.
COPY certs/homelab-rootCA.crt /usr/local/share/ca-certificates/homelab-rootCA.crt
RUN update-ca-certificates

USER airflow
RUN pip install --no-cache-dir \
    "cryptography==45.0.7" \
    "apache-airflow-providers-keycloak==0.11.1" \
    "psycopg2-binary==2.9.10" \
 && pip check
