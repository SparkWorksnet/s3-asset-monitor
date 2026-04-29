package net.sparkworks.datalake.monitor.config;

import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the MinIO client.
 */
@Configuration
public class MinioConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MinioConfiguration.class);

    @Bean
    public MinioClient minioClient(MinioProperties properties) {
        if (properties.getEndpoint() == null || properties.getEndpoint().isBlank()) {
            throw new IllegalStateException("minio.endpoint is required");
        }
        if (properties.getAccessKey() == null || properties.getAccessKey().isBlank()) {
            throw new IllegalStateException("minio.access-key is required");
        }
        if (properties.getSecretKey() == null || properties.getSecretKey().isBlank()) {
            throw new IllegalStateException("minio.secret-key is required");
        }

        logger.info("Creating MinIO client — endpoint: {}", properties.getEndpoint());

        return MinioClient.builder()
                .endpoint(properties.getEndpoint())
                .credentials(properties.getAccessKey(), properties.getSecretKey())
                .build();
    }
}