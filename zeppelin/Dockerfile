FROM apache/zeppelin:0.10.0

USER root
RUN apt-get update && apt-get install -y build-essential \
    && cd /opt \
    && wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz \
    && tar -xvf Python-3.9.9.tgz \
    && cd Python-3.9.9/ \
    && ./configure --prefix=/usr \
    && make \
    && make altinstall

