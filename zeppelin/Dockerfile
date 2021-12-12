FROM apache/zeppelin:0.10.0

USER root
RUN apt-get update && apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev\
    && cd /opt \
    && wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz \
    && tar -xvf Python-3.9.9.tgz \
    && cd Python-3.9.9/ \
    && ./configure --prefix=/usr --enable-optimizations --enable-loadable-sqlite-extensions\
    && make \
    && make altinstall
