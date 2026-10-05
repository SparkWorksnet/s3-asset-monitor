package net.sparkworks.datalake.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the DALI EDC connector Management API.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "dali.connector")
public class DaliConnectorProperties {

    /**
     * Base URL of the DALI EDC connector Management API.
     * Example: http://connector-host:18181
     */
    private String url;

    /**
     * Public URL of the DALI EDC connector, written as {@code dcat:accessURL} on each Piveau
     * distribution (the entrypoint a consumer negotiates through). Falls back to {@link #url}
     * when not set.
     */
    private String accessUrl;

    public String effectiveAccessUrl() {
        return accessUrl != null && !accessUrl.isBlank() ? accessUrl : url;
    }
}
