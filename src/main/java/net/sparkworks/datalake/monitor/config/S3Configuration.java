package net.sparkworks.datalake.monitor.config;

import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the S3 client (the MinIO SDK, which works with any S3-compatible store).
 */
@Configuration
public class S3Configuration {

    private static final Logger logger = LoggerFactory.getLogger(S3Configuration.class);

    @Bean
    public MinioClient minioClient(S3Properties properties) {
        if (properties.getEndpoint() == null || properties.getEndpoint().isBlank()) {
            throw new IllegalStateException("s3.endpoint is required");
        }
        if (properties.getAccessKey() == null || properties.getAccessKey().isBlank()) {
            throw new IllegalStateException("s3.access-key is required");
        }
        if (properties.getSecretKey() == null || properties.getSecretKey().isBlank()) {
            throw new IllegalStateException("s3.secret-key is required");
        }

        logger.info("Creating S3 client — endpoint: {}", properties.getEndpoint());

        return MinioClient.builder()
                .endpoint(properties.getEndpoint())
                .credentials(properties.getAccessKey(), properties.getSecretKey())
                .build();
    }
}