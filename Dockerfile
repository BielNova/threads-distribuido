# ---------- etapa 1: compila o jar (não precisa ter Maven instalado na máquina)
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# ---------- etapa 2: imagem final só com o Java runtime
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/threads.jar app.jar
ENV RESULTADOS=/app/resultados
ENTRYPOINT ["java", "-jar", "app.jar"]
