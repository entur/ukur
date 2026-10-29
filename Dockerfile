FROM bellsoft/liberica-openjdk-debian:21.0.13.0.3-5

RUN addgroup --gid 2000 appuser && adduser --uid 2000 --disabled-password --ingroup appuser appuser
USER appuser

WORKDIR /home/appuser

COPY target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD ["sh", "-c", "java $JAVA_OPTIONS -jar ukur.jar"]