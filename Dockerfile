FROM eclipse-temurin:17-jdk

RUN addgroup appuser && adduser --disabled-password appuser --ingroup appuser
USER appuser

WORKDIR /home/appuser

ADD target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD java $JAVA_OPTIONS -jar ukur.jar