FROM apache/airflow:2.2.1-python3.8

USER root
RUN apt-get update
RUN  apt-get -y install openssl wget

# То, что дописано мной
RUN apt-get install openjdk-11-jdk-headless -qq


RUN mkdir -p /home/jars
RUN wget -O /home/jars/driver.jar https://jdbc.postgresql.org/download/postgresql-42.3.1.jar

USER airflow
RUN pip install pyspark==3.2.0