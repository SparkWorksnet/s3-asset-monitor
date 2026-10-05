package net.sparkworks.datalake.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for connecting to S3-compatible storage (MinIO, RustFS, AWS S3, ...).
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "s3")
public class S3Properties {

    /** S3 endpoint, e.g. http://localhost:9000 */
    private String endpoint;

    /** S3 access key */
    private String accessKey;

    /** S3 secret key */
    private String secretKey;
}