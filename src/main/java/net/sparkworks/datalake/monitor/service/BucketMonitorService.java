package net.sparkworks.datalake.monitor.service;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import jakarta.annotation.PostConstruct;
import net.sparkworks.datalake.monitor.config.MonitorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Periodically polls configured MinIO buckets for new files.
 * On startup all existing objects are marked as seen without being registered,
 * so only files that appear after the monitor starts will be registered as EDC assets.
 */
@Service
public class BucketMonitorService {

    private static final Logger logger = LoggerFactory.getLogger(BucketMonitorService.class);

    private final MinioClient minioClient;
    private final MonitorProperties monitorProperties;
    private final EdcAssetRegistrationService registrationService;

    /** Tracks bucket:objectKey pairs that have already been registered (or intentionally skipped on startup). */
    private final Set<String> seenKeys = ConcurrentHashMap.newKeySet();

    private volatile boolean initialScanDone = false;

    public BucketMonitorService(MinioClient minioClient,
                                MonitorProperties monitorProperties,
                                EdcAssetRegistrationService registrationService) {
        this.minioClient = minioClient;
        this.monitorProperties = monitorProperties;
        this.registrationService = registrationService;
    }

    /**
     * On startup: scan all configured buckets and mark every existing matching file as seen.
     * This prevents re-registering files that were already present before the monitor started.
     */
    @PostConstruct
    public void initialScan() {
        logger.info("S3 Asset Monitor starting — scanning {} bucket(s) for existing files",
                monitorProperties.getBuckets().size());

        for (String bucket : monitorProperties.getBuckets()) {
            try {
                List<String> existingKeys = listMatchingObjects(bucket);
                for (String key : existingKeys) {
                    seenKeys.add(seenKey(bucket, key));
                }
                logger.info("  Bucket '{}': {} existing file(s) marked as seen (skipped)", bucket, existingKeys.size());
            } catch (Exception e) {
                logger.warn("  Bucket '{}': initial scan failed — {}", bucket, e.getMessage());
            }
        }

        initialScanDone = true;
        logger.info("Initial scan complete. Polling every {} second(s).", monitorProperties.getPollIntervalSeconds());
    }

    /**
     * Periodic poll: for each bucket, find files not yet seen and register them.
     * The fixed-delay is expressed in milliseconds; the configured value is in seconds.
     */
    @Scheduled(fixedDelayString = "#{monitorProperties.pollIntervalSeconds * 1000L}")
    public void poll() {
        if (!initialScanDone) {
            return;
        }

        for (String bucket : monitorProperties.getBuckets()) {
            try {
                List<String> objectKeys = listMatchingObjects(bucket);
                int newCount = 0;

                for (String key : objectKeys) {
                    String sk = seenKey(bucket, key);
                    if (seenKeys.add(sk)) {
                        logger.info("New file detected — bucket: '{}', key: '{}'", bucket, key);
                        registrationService.registerAsset(key, key, bucket);
                        newCount++;
                    }
                }

                if (newCount > 0) {
                    logger.info("Bucket '{}': registered {} new file(s)", bucket, newCount);
                } else {
                    logger.debug("Bucket '{}': no new files", bucket);
                }

            } catch (Exception e) {
                logger.warn("Failed to poll bucket '{}': {}", bucket, e.getMessage());
            }
        }
    }

    /**
     * List all objects in {@code bucket} whose names end with one of the configured extensions.
     */
    private List<String> listMatchingObjects(String bucket) {
        List<String> matched = new ArrayList<>();
        List<String> extensions = monitorProperties.getFileExtensions();

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(bucket)
                        .recursive(true)
                        .build()
        );

        for (Result<Item> result : results) {
            try {
                Item item = result.get();
                if (item.isDir()) continue;
                String key = item.objectName();
                String keyLower = key.toLowerCase();
                for (String ext : extensions) {
                    if (keyLower.endsWith(ext)) {
                        matched.add(key);
                        break;
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to read object metadata: {}", e.getMessage());
            }
        }

        return matched;
    }

    private String seenKey(String bucket, String objectKey) {
        return bucket + ":" + objectKey;
    }
}