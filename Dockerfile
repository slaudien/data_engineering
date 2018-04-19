FROM centos:centos7

# Install some packages
RUN yum install -y wget tar openssh-server openssh-clients sysstat sudo which openssl hostname
# Install Java
RUN yum install -y java-1.8.0-openjdk-headless
RUN yum clean all

ENV HOME /home
WORKDIR $HOME

# Download and Install Spark
ENV SPARK_VERSION=2.3.0
ENV HADOOP_VERSION=2.7
RUN wget http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz 

ENV SPARK_HOME $HOME/spark

