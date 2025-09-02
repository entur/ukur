FROM eclipse-temurin:21-noble

RUN addgroup --gid 2000 appuser && adduser --uid 2000 --disabled-password --ingroup appuser appuser
USER appuser

WORKDIR /home/appuser

ADD target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD java $JAVA_OPTIONS -jar ukur.jar