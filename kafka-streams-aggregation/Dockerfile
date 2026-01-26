FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre-alpine

WORKDIR /app
COPY --from=build /app/target/log-aggregation-streams-1.0.0.jar /app/app.jar

ENV TOP_N=3
ENV PERIOD_MINUTES=5
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV CASSANDRA_HOST=cassandra

CMD ["java", "-jar", "app.jar"]
