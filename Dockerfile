FROM java:8-jre-alpine

ADD target/java-datasource-template-1.0-SNAPSHOT.jar /srv/

ENTRYPOINT ["java", "-jar", "/srv/java-datasource-template-1.0-SNAPSHOT.jar"]


