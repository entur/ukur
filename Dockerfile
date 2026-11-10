FROM bellsoft/liberica-openjdk-debian:27-36@sha256:c8b4fe2c477ee36b7666c4a036c8b224cc265371a9992fbf69e356ca3002096f

RUN addgroup --gid 2000 appuser && adduser --uid 2000 --disabled-password --ingroup appuser appuser
USER appuser

WORKDIR /home/appuser

COPY target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD ["sh", "-c", "java $JAVA_OPTIONS -jar ukur.jar"]