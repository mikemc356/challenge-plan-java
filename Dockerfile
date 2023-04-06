FROM eclipse-temurin:17-jdk-alpine
VOLUME /tmp
RUN mvn pom.xml clean package
COPY target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
