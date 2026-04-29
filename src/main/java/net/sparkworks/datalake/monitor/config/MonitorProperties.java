package net.sparkworks.datalake.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for the bucket monitor.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "monitor")
public class MonitorProperties {

    /**
     * List of MinIO bucket names to monitor.
     * Each bucket is scanned independently on every poll cycle.
     */
    private List<String> buckets = new ArrayList<>();

    /**
     * How often to poll each bucket for new files, in seconds.
     */
    private long pollIntervalSeconds = 30;

    /**
     * File extensions to watch (lowercase, including the dot).
     * Files whose names end with any of these extensions will be registered.
     */
    private List<String> fileExtensions = List.of(".csv");
}