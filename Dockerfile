FROM maven:3.6.0-jdk-11-slim AS build

ARG GITHUB_USER
ARG GITHUB_TOKEN

RUN mkdir /project
WORKDIR /project

COPY settings.xml .
COPY src src
COPY pom.xml .
COPY bin bin
RUN mvn clean test package --settings settings.xml