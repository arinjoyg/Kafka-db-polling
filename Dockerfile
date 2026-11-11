FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/kafka-rr-sla-*.jar app.jar
# Container-aware heap; ZGC keeps pause times well under the latency budget.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseZGC -XX:+ZGenerational"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
