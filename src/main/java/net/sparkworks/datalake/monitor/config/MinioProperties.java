package net.sparkworks.datalake.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for connecting to MinIO / S3-compatible storage.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "minio")
public class MinioProperties {

    /** MinIO server endpoint, e.g. http://localhost:9000 */
    private String endpoint;

    /** MinIO access key */
    private String accessKey;

    /** MinIO secret key */
    private String secretKey;
}