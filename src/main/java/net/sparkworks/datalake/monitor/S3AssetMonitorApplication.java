package net.sparkworks.datalake.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot application that monitors S3/MinIO buckets for new CSV files
 * and registers them as assets in the DALI EDC connector.
 */
@SpringBootApplication
@EnableScheduling
public class S3AssetMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(S3AssetMonitorApplication.class, args);
    }
}