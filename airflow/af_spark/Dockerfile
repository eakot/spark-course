FROM bitnami/spark:3.2.0

USER root

RUN apt-get update
RUN apt-get install -y openssl wget

# RUN mkdir -p /jars
RUN wget "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar"
# RUN mv postgresql-42.3.1.jar /jars/
RUN mv postgresql-42.3.1.jar /opt/bitnami/spark/jars/
RUN chmod 777 -R /opt/bitnami/spark/jars/
# RUN chmod 777 /opt/bitnami/spark/jars/postgresql-42.3.1.jar
