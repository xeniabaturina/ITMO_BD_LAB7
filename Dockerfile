FROM openjdk:17-jdk-slim AS builder

# Install curl, gnupg and sbt
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --dearmor | tee /etc/apt/trusted.gpg.d/sbt.gpg > /dev/null \
    && apt-get update \
    && apt-get install -y sbt \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy build files
COPY build.sbt .
COPY project/ project/

# Download dependencies
RUN sbt update

# Copy source code
COPY src/ src/

# Build the application
RUN sbt assembly

# Production stage
FROM eclipse-temurin:17-jre

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Create logs directory
RUN mkdir -p logs

# Copy the built JAR from builder stage
COPY --from=builder /app/target/scala-2.13/food-clustering-datamart.jar /app/

# Copy configuration
COPY src/main/resources/application.conf /app/application.conf

# Create non-root user
RUN groupadd -r datamart && useradd -r -g datamart datamart
RUN chown -R datamart:datamart /app
USER datamart

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Set JVM options
ENV JVM_OPTS="-Xmx2g -Xms1g -XX:+UseG1GC -XX:+UseStringDeduplication"

# Default command
ENTRYPOINT ["sh", "-c", "java $JVM_OPTS -Dconfig.file=/app/application.conf -jar /app/food-clustering-datamart.jar $0 $@"]
CMD ["server"]
