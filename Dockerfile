# Multi-stage build for camellia-sync (single jar, app.role decides runtime behaviour)
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Layer 1: dependency cache (only poms first)
COPY pom.xml .
COPY sync-common/pom.xml sync-common/
COPY sync-proxy/pom.xml sync-proxy/
COPY sync-app/pom.xml sync-app/
RUN mvn -q -B -DskipTests dependency:go-offline || true

# Layer 2: sources + build
COPY sync-common/src sync-common/src
COPY sync-proxy/src sync-proxy/src
COPY sync-app/src sync-app/src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/sync-app/target/camellia-sync.jar app.jar
# 8080 = Web REST (server.port), 6380 = Redis proxy (camellia config.port), 16379 = console
EXPOSE 8080 6380 16379
ENTRYPOINT ["java", "-jar", "app.jar"]
