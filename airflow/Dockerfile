FROM apache/airflow:2.2.1-python3.8

USER root
RUN apt-get update
RUN apt-get install -y openssl wget
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN apt update -y && apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && \
    apt update -y && \
    apt-get install -y openjdk-8-jdk-headless && \
    export JAVA_HOME && \
    apt-get clean

USER airflow
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir apache-airflow-providers-apache-spark

RUN pip install --force-reinstall --no-cache-dir pyspark==3.2.0

RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir plyvel loguru fire
