FROM openjdk:11.0.11-jre-slim-buster as builder

RUN apt-get update 
RUN apt-get install -y wget tar bash curl nano software-properties-common 

# python3
RUN apt-get install -y python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy

# R
# RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
# RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
# RUN apt install -y dirmngr gnupg apt-transport-https ca-certificates software-properties-common
# RUN apt install -y r-base

# Apache Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.2.1-bin-hadoop3.2.tgz && \
    mv spark-3.2.1-bin-hadoop3.2/ /opt/spark && \
    rm spark-3.2.1-bin-hadoop3.2.tgz

# web UI master
EXPOSE 8080 

# web UI workers
EXPOSE 8081

# web UI Apache Spark
EXPOSE 7077

# RUN mkdir /app
# WORKDIR /app
