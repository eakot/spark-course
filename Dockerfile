ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=11
ARG PYTHON_VERSION=3.9.5

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /
WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Create logs directory
RUN mkdir -p logs

# Install wget and download jars for Spark
RUN apt-get update && apt install -y wget
RUN mkdir -p jars
RUN wget "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar"
RUN mv postgresql-42.3.1.jar jars/
