FROM maven:3-amazoncorretto-21-alpine AS build

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
