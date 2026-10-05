# Multi-stage build for S3 Asset Monitor
# Stage 1: Build the application
FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /build

# Copy Maven files for dependency caching
COPY pom.xml .

# Download dependencies (cached layer)
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application (skip tests for faster builds)
RUN mvn clean package -DskipTests -B

# Split the jar into layers (dependencies / loader / snapshots / application), so a code
# change only produces a small new image layer instead of re-shipping every dependency.
# All four directories are created up front because a layer with no content (usually
# snapshot-dependencies) is not extracted, and the COPY below needs it to exist.
RUN cp target/s3-asset-monitor-*.jar app.jar && \
    mkdir -p extracted/dependencies extracted/spring-boot-loader \
             extracted/snapshot-dependencies extracted/application && \
    java -Djarmode=layertools -jar app.jar extract --destination extracted

# Stage 2: Runtime image
FROM eclipse-temurin:17-jre-alpine

# Set working directory
WORKDIR /app

# Install curl for healthchecks
RUN apk add --no-cache curl

# Create non-root user for security
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser && \
    chown -R appuser:appuser /app

# Copy the application layers, least to most frequently changing so Docker reuses the
# big, stable ones across builds and pulls
COPY --from=builder --chown=appuser:appuser /build/extracted/dependencies/ ./
COPY --from=builder --chown=appuser:appuser /build/extracted/spring-boot-loader/ ./
COPY --from=builder --chown=appuser:appuser /build/extracted/snapshot-dependencies/ ./
COPY --from=builder --chown=appuser:appuser /build/extracted/application/ ./

# Switch to non-root user
USER appuser

# Expose the application port
EXPOSE 4001

# JVM options. Size the heap from the container's memory limit rather than the host's, and
# exit on OutOfMemoryError so the container's restart policy brings it back cleanly instead
# of leaving a half-working JVM. Override JAVA_OPTS to change them.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:4001/actuator/health || exit 1

# Run the application. A shell is used so JAVA_OPTS is expanded; exec makes the JVM PID 1
# so it receives SIGTERM and shuts down gracefully.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
