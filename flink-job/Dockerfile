FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.19.1-scala_2.12-java17

COPY --from=build /build/target/pipeline-flink-job.jar /opt/flink/usrlib/pipeline-flink-job.jar
