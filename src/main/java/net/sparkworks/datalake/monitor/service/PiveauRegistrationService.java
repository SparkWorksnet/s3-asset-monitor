package net.sparkworks.datalake.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import net.sparkworks.datalake.monitor.config.DaliConnectorProperties;
import net.sparkworks.datalake.monitor.config.MinioProperties;
import net.sparkworks.datalake.monitor.config.MonitorProperties;
import net.sparkworks.datalake.monitor.piveau.DatasetMetadata;
import net.sparkworks.datalake.monitor.piveau.DcatTurtleBuilder;
import net.sparkworks.datalake.monitor.piveau.PiveauClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers the datasets and distributions found in the data lake with the Piveau catalogue.
 *
 * <p>The data lake layout, as written by the testbed connector's transfer, is
 * {@code <bucket>/<experiment-id>/<file>}. The bucket names the Piveau catalogue and the
 * directory the dataset id. A {@code metadata.json} in the directory describes the dataset;
 * every data file in it becomes a distribution of that dataset.
 *
 * <p>Both operations are written to be retried by the caller: {@link #registerDataset} is an
 * idempotent PUT, and {@link #registerDistribution} returns {@code false} while the dataset is
 * not registered yet.
 */
@Service
public class PiveauRegistrationService {

    private static final Logger logger = LoggerFactory.getLogger(PiveauRegistrationService.class);

    private static final String DEFAULT_LICENSE = "https://creativecommons.org/licenses/by/4.0/";

    private final PiveauClient piveau;
    private final MinioClient minioClient;
    private final MinioProperties minioProperties;
    private final MonitorProperties monitorProperties;
    private final DaliConnectorProperties connectorProperties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Metadata of each dataset registered by this process. A distribution's own columns,
     * measurement technique and licence come from its dataset's metadata.json, which is
     * re-read from the data lake on a cache miss (e.g. after a restart).
     */
    private final Map<String, DatasetMetadata> metadataCache = new ConcurrentHashMap<>();

    public PiveauRegistrationService(PiveauClient piveau, MinioClient minioClient, MinioProperties minioProperties,
                                     MonitorProperties monitorProperties, DaliConnectorProperties connectorProperties) {
        this.piveau = piveau;
        this.minioClient = minioClient;
        this.minioProperties = minioProperties;
        this.monitorProperties = monitorProperties;
        this.connectorProperties = connectorProperties;
    }

    public boolean isEnabled() {
        return piveau.isEnabled();
    }

    /** Whether an object key is a dataset's metadata file. */
    public boolean isMetadataFile(String objectKey) {
        return extractFileName(objectKey).equalsIgnoreCase(monitorProperties.getMetadataFileName());
    }

    /**
     * The dataset id for an object: the directory directly containing it, or {@code null} for
     * an object at the root of the bucket (which belongs to no dataset).
     */
    public String datasetId(String objectKey) {
        String[] parts = objectKey.split("/");
        return parts.length > 1 ? parts[parts.length - 2] : null;
    }

    /**
     * Register (or update) the dataset described by a {@code metadata.json}.
     *
     * @throws RuntimeException if the metadata cannot be read or Piveau rejects it; the caller retries
     */
    public void registerDataset(String bucket, String metadataKey) throws Exception {
        String datasetId = datasetId(metadataKey);
        if (datasetId == null) {
            logger.warn("Ignoring '{}' in bucket '{}': a metadata file must sit inside a dataset directory",
                    metadataKey, bucket);
            return;
        }

        DatasetMetadata metadata = readMetadata(bucket, metadataKey);
        // The directory name is the fallback for dct:identifier and the title when the JSON omits them.
        metadata.setDatasetId(datasetId);

        String today = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        String issued = metadata.getIssued() != null ? metadata.getIssued() : today;
        // dct:modified is the issued date on first registration.
        String turtle = DcatTurtleBuilder.buildDataset(piveau.apiUrl(), datasetId, metadata, issued, issued);

        logger.debug("Dataset Turtle for '{}':\n{}", datasetId, turtle);
        piveau.putDataset(datasetId, bucket, turtle);
        metadataCache.put(cacheKey(bucket, datasetId), metadata);
        logger.info("✓ Dataset '{}' registered in Piveau catalogue '{}'", datasetId, bucket);
    }

    /**
     * Add a data file to its dataset as a distribution.
     *
     * @param assetId the id the file is registered under as an EDC asset, written as {@code dali:assetId}
     * @return {@code false} if the dataset is not in the catalogue yet (retry later), {@code true} once created
     */
    public boolean registerDistribution(String bucket, String objectKey, String assetId) throws Exception {
        String datasetId = datasetId(objectKey);
        if (datasetId == null) {
            logger.warn("Not creating a distribution for '{}' in bucket '{}': it is not inside a dataset directory",
                    objectKey, bucket);
            return true;
        }
        if (!piveau.datasetExists(datasetId, bucket)) {
            logger.info("Dataset '{}' is not in Piveau yet — distribution for '{}' will be retried", datasetId, objectKey);
            return false;
        }

        String fileName = extractFileName(objectKey);
        DatasetMetadata metadata = metadataFor(bucket, objectKey, datasetId);
        String license = metadata != null && StringUtils.hasText(metadata.getLicense())
                ? metadata.getLicense() : DEFAULT_LICENSE;

        String turtle = DcatTurtleBuilder.buildDistribution(
                piveau.apiUrl(), datasetId, DcatTurtleBuilder.distributionId(datasetId + "-" + fileName), assetId,
                fileName, connectorProperties.effectiveAccessUrl(), downloadUrl(bucket, objectKey),
                metadata != null ? metadata.getColumns() : null,
                metadata != null ? metadata.getMeasurementTechnique() : null,
                license, LocalDate.now().format(DateTimeFormatter.ISO_DATE));

        logger.debug("Distribution Turtle for '{}':\n{}", objectKey, turtle);
        piveau.postDistribution(datasetId, turtle);
        logger.info("✓ Distribution '{}' created in Piveau dataset '{}'", fileName, datasetId);
        return true;
    }

    private DatasetMetadata metadataFor(String bucket, String objectKey, String datasetId) {
        DatasetMetadata cached = metadataCache.get(cacheKey(bucket, datasetId));
        if (cached != null) {
            return cached;
        }
        int lastSlash = objectKey.lastIndexOf('/');
        String metadataKey = objectKey.substring(0, lastSlash + 1) + monitorProperties.getMetadataFileName();
        try {
            DatasetMetadata metadata = readMetadata(bucket, metadataKey);
            metadata.setDatasetId(datasetId);
            metadataCache.put(cacheKey(bucket, datasetId), metadata);
            return metadata;
        } catch (Exception e) {
            logger.warn("Could not read '{}' from bucket '{}' for distribution defaults: {}", metadataKey, bucket, e.getMessage());
            return null;
        }
    }

    private DatasetMetadata readMetadata(String bucket, String metadataKey) throws Exception {
        try (InputStream in = minioClient.getObject(GetObjectArgs.builder().bucket(bucket).object(metadataKey).build())) {
            String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return objectMapper.readValue(json, DatasetMetadata.class);
        }
    }

    /** The file's actual S3 URL, written as dcat:downloadURL (distinct from dcat:accessURL, the connector). */
    private String downloadUrl(String bucket, String objectKey) {
        String endpoint = minioProperties.getEndpoint();
        if (!StringUtils.hasText(endpoint)) {
            return null;
        }
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return base + "/" + bucket + "/" + objectKey;
    }

    private static String cacheKey(String bucket, String datasetId) {
        return bucket + ":" + datasetId;
    }

    private static String extractFileName(String objectKey) {
        int lastSlash = objectKey.lastIndexOf('/');
        return lastSlash >= 0 ? objectKey.substring(lastSlash + 1) : objectKey;
    }
}
