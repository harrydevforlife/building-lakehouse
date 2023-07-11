FROM python:3.9-slim-buster

LABEL maintainer "admin@lakehouse.io"

RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install openjdk-11-jdk -yqq

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin


RUN pip install mlflow==2.3.2 && \
    pip install awscli --upgrade --user && \
    pip install boto3==1.16.46 && \
    pip install flask && \
    pip install pyspark==3.3.2

ENV PORT 5000

COPY files/run.sh /
COPY main.py $HOME/main.py

ENTRYPOINT ["/run.sh"]
