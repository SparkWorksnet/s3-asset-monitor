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
}