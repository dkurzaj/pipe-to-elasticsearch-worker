FROM maven:3.5.4-jdk-10

COPY ./pipeToElasticsearch /usr/src/myapp/
WORKDIR /usr/src/myapp/

RUN apt-get update
RUN apt-get install -y kafkacat

RUN mvn clean package assembly:single

ENTRYPOINT ["java", "-jar", "target/pipeToElasticsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar"]
