package net.sparkworks.datalake.monitor.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Periodically polls configured MinIO buckets for new files.
 * On startup all existing objects are marked as seen without being registered,
 * so only files that appear after the monitor starts will be registered as EDC assets.
 *
 * <p>Exposed Prometheus metrics:
 * <ul>
 *   <li>{@code s3_monitor_poll_total{bucket}} — poll cycles executed per bucket</li>
 *   <li>{@code s3_monitor_files_discovered_total{bucket}} — matching files seen per poll</li>
 *   <li>{@code s3_monitor_files_registered_total{bucket}} — newly registered files</li>
 *   <li>{@code s3_monitor_registration_errors_total{bucket}} — failed registrations</li>
 *   <li>{@code s3_monitor_seen_keys} — gauge: total size of the seen-keys set</li>
 *   <li>{@code s3_monitor_poll_duration_seconds{bucket}} — wall-clock time per poll cycle</li>
 * </ul>
 */
@Service
public class BucketMonitorService {

    private static final Logger logger = LoggerFactory.getLogger(BucketMonitorService.class);

    private final MinioClient minioClient;
    private final MonitorProperties monitorProperties;
    private final EdcAssetRegistrationService registrationService;
    private final MeterRegistry meterRegistry;

    /** Tracks bucket:objectKey pairs that have already been registered (or skipped on startup). */
    private final Set<String> seenKeys = ConcurrentHashMap.newKeySet();

    /** Per-bucket counters and timers, created lazily on first poll of each bucket. */
    private final Map<String, Counter> pollCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> discoveredCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> registeredCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> pollTimers = new ConcurrentHashMap<>();

    private volatile boolean initialScanDone = false;

    public BucketMonitorService(MinioClient minioClient,
                                MonitorProperties monitorProperties,
                                EdcAssetRegistrationService registrationService,
                                MeterRegistry meterRegistry) {
        this.minioClient = minioClient;
        this.monitorProperties = monitorProperties;
        this.registrationService = registrationService;
        this.meterRegistry = meterRegistry;
    }

    /**
     * On startup: scan all configured buckets and mark every existing matching file as seen.
     * Also registers the seen-keys gauge so Prometheus can track set growth over time.
     */
    @PostConstruct
    public void initialScan() {
        // Gauge: total number of keys tracked across all buckets
        meterRegistry.gauge("s3_monitor_seen_keys", seenKeys, Set::size);

        logger.info("S3 Asset Monitor starting — scanning {} bucket(s) for existing files",
                monitorProperties.getBuckets().size());

        for (String bucket : monitorProperties.getBuckets()) {
            // Eagerly create per-bucket meters so they appear in Prometheus from the start
            metersForBucket(bucket);

            try {
                List<String> existingKeys = listMatchingObjects(bucket);
                for (String key : existingKeys) {
                    seenKeys.add(seenKey(bucket, key));
                }
                logger.info("  Bucket '{}': {} existing file(s) marked as seen (skipped)",
                        bucket, existingKeys.size());
            } catch (Exception e) {
                logger.warn("  Bucket '{}': initial scan failed — {}", bucket, e.getMessage());
            }
        }

        initialScanDone = true;
        logger.info("Initial scan complete. Polling every {} second(s).",
                monitorProperties.getPollIntervalSeconds());
    }

    /**
     * Periodic poll: for each bucket, find files not yet seen and register them.
     * Fixed-delay in milliseconds; the configured value is in seconds.
     */
    @Scheduled(fixedDelayString = "#{monitorProperties.pollIntervalSeconds * 1000L}")
    public void poll() {
        if (!initialScanDone) {
            return;
        }

        for (String bucket : monitorProperties.getBuckets()) {
            Meters m = metersForBucket(bucket);
            m.pollCounter.increment();

            m.pollTimer.record(() -> pollBucket(bucket, m));
        }
    }

    private void pollBucket(String bucket, Meters m) {
        try {
            List<String> objectKeys = listMatchingObjects(bucket);
            m.discoveredCounter.increment(objectKeys.size());

            int newCount = 0;
            for (String key : objectKeys) {
                String sk = seenKey(bucket, key);
                if (seenKeys.add(sk)) {
                    logger.info("New file detected — bucket: '{}', key: '{}'", bucket, key);
                    try {
                        registrationService.registerAsset(key, key, bucket);
                        m.registeredCounter.increment();
                        newCount++;
                    } catch (Exception e) {
                        m.errorCounter.increment();
                        logger.warn("Failed to register '{}' in bucket '{}': {}", key, bucket, e.getMessage());
                        // Remove from seen so it is retried on the next poll
                        seenKeys.remove(sk);
                    }
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

    // ── Meter helpers ──────────────────────────────────────────────────────────

    private record Meters(Counter pollCounter, Counter discoveredCounter,
                          Counter registeredCounter, Counter errorCounter,
                          Timer pollTimer) {}

    private Meters metersForBucket(String bucket) {
        return new Meters(
                pollCounters.computeIfAbsent(bucket, b -> Counter.builder("s3_monitor_poll_total")
                        .description("Number of poll cycles executed")
                        .tag("bucket", b)
                        .register(meterRegistry)),
                discoveredCounters.computeIfAbsent(bucket, b -> Counter.builder("s3_monitor_files_discovered_total")
                        .description("Number of matching files seen during polls")
                        .tag("bucket", b)
                        .register(meterRegistry)),
                registeredCounters.computeIfAbsent(bucket, b -> Counter.builder("s3_monitor_files_registered_total")
                        .description("Number of new files successfully registered as EDC assets")
                        .tag("bucket", b)
                        .register(meterRegistry)),
                errorCounters.computeIfAbsent(bucket, b -> Counter.builder("s3_monitor_registration_errors_total")
                        .description("Number of failed EDC asset registrations")
                        .tag("bucket", b)
                        .register(meterRegistry)),
                pollTimers.computeIfAbsent(bucket, b -> Timer.builder("s3_monitor_poll_duration_seconds")
                        .description("Wall-clock time spent polling a bucket")
                        .tag("bucket", b)
                        .register(meterRegistry))
        );
    }
}
