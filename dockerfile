FROM apache/airflow:2.6.1
USER root

ARG AIRFLOW_HOME=/usr/local/stock_flow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV YARN_CONF_DIR $HADOOP_HOME/cluster-conf

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.0



