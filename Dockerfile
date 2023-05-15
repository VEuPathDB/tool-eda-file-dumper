FROM maven:3.8.6-jdk-11-slim AS build

ARG GITHUB_USER
ARG GITHUB_TOKEN

RUN mkdir /project
WORKDIR /project

COPY settings.xml .
COPY src src
COPY pom.xml .
COPY bin bin
RUN mvn clean test package --settings settings.xml

ENV PATH=/project/bin:$PATH

# RUN apt-get update --fix-missing && apt-get install -y \
#     wget \
#     unzip


# WORKDIR /opt/oracle

# RUN export INSTANTCLIENT_VER=linux.x64-21.6.0.0.0dbru \
#     && wget https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-basic-$INSTANTCLIENT_VER.zip \
#     && wget https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-sqlplus-$INSTANTCLIENT_VER.zip \
#     && unzip instantclient-sqlplus-$INSTANTCLIENT_VER.zip \
#     && unzip instantclient-basic-$INSTANTCLIENT_VER.zip

# # Need to change this if we get new version of the instantclient
# ENV ORACLE_HOME=/opt/oracle/instantclient_21_6
# ENV LD_LIBRARY_PATH=$ORACLE_HOME


# WORKDIR /project
