FROM bellsoft/liberica-openjdk-debian:21.0.13.0.3-5@sha256:5b02c73e1d01a9feb1a4c991023c3b301bcd4cd561519ec063ebbfb985f6c94f

RUN addgroup --gid 2000 appuser && adduser --uid 2000 --disabled-password --ingroup appuser appuser
USER appuser

WORKDIR /home/appuser

COPY target/ukur-*-SNAPSHOT.jar ukur.jar

EXPOSE 8080
CMD ["sh", "-c", "java $JAVA_OPTIONS -jar ukur.jar"]