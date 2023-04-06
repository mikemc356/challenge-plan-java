FROM maven:3.6.0-jdk-11-slim
VOLUME /tmp
RUN mvn pom.xml clean package
COPY target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
