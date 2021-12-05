ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=11
ARG PYTHON_VERSION=3.9.5

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
#--mount=type=cache,target=/root/.cache \

CMD ["spark_task_hw/src/task.py"]
ENTRYPOINT ["python3"]
