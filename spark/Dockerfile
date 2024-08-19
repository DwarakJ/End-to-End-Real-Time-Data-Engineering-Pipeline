FROM bitnami/spark:3.5.0


WORKDIR /opt/bitnami/spark

RUN pip install py4j

COPY spark_processing.py ./spark_processing.py

ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=postgres

