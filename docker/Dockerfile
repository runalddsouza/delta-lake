FROM python:3.9-slim-buster

# OpenJdk for spark-submit
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get install -y zip

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# PySpark
RUN pip install pyspark==3.2.0
