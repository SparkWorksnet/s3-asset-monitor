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
import net.sparkworks.datalake.monitor.config.PiveauProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Periodically polls configured S3 buckets for new files and handles each one:
 * <ul>
 *   <li>a dataset's {@code metadata.json} is registered as a dataset in the Piveau catalogue;</li>
 *   <li>a data file (see {@code monitor.file-extensions}) is registered as an EDC asset and then
 *       added to its dataset in Piveau as a distribution.</li>
 * </ul>
 * On startup all existing objects are marked as seen without being registered,
 * so only files that appear after the monitor starts are handled.
 *
 * <p>A file whose handling fails (or whose dataset is not in Piveau yet) is left unseen and
 * retried on the next poll, up to {@code piveau.max-attempts} times.
 *
 * <p>Exposed Prometheus metrics:
 * <ul>
 *   <li>{@code s3_monitor_poll_total{bucket}} — poll cycles executed per bucket</li>
 *   <li>{@code s3_monitor_files_discovered_total{bucket}} — matching files seen per poll</li>
 *   <li>{@code s3_monitor_files_registered_total{bucket}} — files fully handled (asset and catalogue)</li>
 *   <li>{@code s3_monitor_registration_errors_total{bucket}} — failed registration attempts</li>
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
    private final PiveauRegistrationService piveauService;
    private final PiveauProperties piveauProperties;
    private final MeterRegistry meterRegistry;

    /** Tracks bucket:objectKey pairs that have already been registered (or skipped on startup). */
    private final Set<String> seenKeys = ConcurrentHashMap.newKeySet();

    /** Failed or not-yet-possible attempts per bucket:objectKey, to cap retries. */
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

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
                                PiveauRegistrationService piveauService,
                                PiveauProperties piveauProperties,
                                MeterRegistry meterRegistry) {
        this.minioClient = minioClient;
        this.monitorProperties = monitorProperties;
        this.registrationService = registrationService;
        this.piveauService = piveauService;
        this.piveauProperties = piveauProperties;
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

            // A dataset's metadata.json must be registered before its data files can be added to
            // it as distributions, and the listing is alphabetical, so handle metadata files first.
            List<String> ordered = new ArrayList<>(objectKeys);
            ordered.sort(Comparator.comparing(piveauService::isMetadataFile).reversed());

            int newCount = 0;
            for (String key : ordered) {
                String sk = seenKey(bucket, key);
                if (!seenKeys.add(sk)) {
                    continue;
                }
                logger.info("New file detected — bucket: '{}', key: '{}'", bucket, key);
                try {
                    if (handleFile(bucket, key)) {
                        attempts.remove(sk);
                        m.registeredCounter.increment();
                        newCount++;
                    } else {
                        retryLater(sk, bucket, key, "its dataset is not in Piveau yet");
                    }
                } catch (Exception e) {
                    m.errorCounter.increment();
                    logger.warn("Failed to handle '{}' in bucket '{}': {}", key, bucket, e.getMessage());
                    retryLater(sk, bucket, key, e.getMessage());
                }
            }

            if (newCount > 0) {
                logger.info("Bucket '{}': handled {} new file(s)", bucket, newCount);
            } else {
                logger.debug("Bucket '{}': no new files", bucket);
            }

        } catch (Exception e) {
            logger.warn("Failed to poll bucket '{}': {}", bucket, e.getMessage());
        }
    }

    /**
     * Handle one new object.
     *
     * @return {@code true} when done, {@code false} when it cannot be completed yet and should be retried
     */
    private boolean handleFile(String bucket, String key) throws Exception {
        if (piveauService.isMetadataFile(key)) {
            piveauService.registerDataset(bucket, key);
            return true;
        }

        String assetId = registrationService.deriveAssetId(key);
        registrationService.registerAsset(assetId, key, bucket);
        return !piveauService.isEnabled() || piveauService.registerDistribution(bucket, key, assetId);
    }

    /**
     * Leave a file unseen so the next poll picks it up again, unless it has used up its attempts,
     * in which case it stays seen (skipped until the monitor restarts).
     */
    private void retryLater(String seenKey, String bucket, String key, String reason) {
        int attempt = attempts.merge(seenKey, 1, Integer::sum);
        if (attempt >= piveauProperties.getMaxAttempts()) {
            attempts.remove(seenKey);
            logger.warn("Giving up on '{}' in bucket '{}' after {} attempts: {}", key, bucket, attempt, reason);
            return;
        }
        seenKeys.remove(seenKey);
    }

    /**
     * List all objects in {@code bucket} whose names end with one of the configured extensions, plus
     * the dataset metadata files when Piveau registration is enabled.
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
                if (piveauService.isEnabled() && piveauService.isMetadataFile(key)) {
                    matched.add(key);
                    continue;
                }
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
