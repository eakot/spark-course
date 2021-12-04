FROM apache/zeppelin:0.8.2


RUN wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz \
    && tar -xvf Python-3.9.9.tgz \
    && cd Python-3.9.9/ \
    && ./configure --prefix=/usr \
    && make \
    && make altinstall

RUN cd /tmp
RUN mkdir -p jars
RUN wget "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar" --no-check-certificate
RUN mv postgresql-42.3.1.jar jars/