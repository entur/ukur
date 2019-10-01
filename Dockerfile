FROM openjdk:11-jre
ADD target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD java $JAVA_OPTIONS -jar /ukur.jar