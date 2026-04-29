package net.sparkworks.datalake.monitor.service;

import net.sparkworks.datalake.monitor.config.DaliConnectorProperties;
import net.sparkworks.datalake.monitor.config.MinioProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Registers files discovered in MinIO as assets in the DALI EDC connector Management API.
 */
@Service
public class EdcAssetRegistrationService {

    private static final Logger logger = LoggerFactory.getLogger(EdcAssetRegistrationService.class);

    private final DaliConnectorProperties connectorProperties;
    private final MinioProperties minioProperties;
    private final RestClient restClient;

    public EdcAssetRegistrationService(DaliConnectorProperties connectorProperties,
                                       MinioProperties minioProperties) {
        this.connectorProperties = connectorProperties;
        this.minioProperties = minioProperties;
        this.restClient = RestClient.create();
        logger.info("EdcAssetRegistrationService initialized — connector URL: {}", connectorProperties.getUrl());
    }

    /**
     * Register a MinIO object as an asset in the DALI EDC connector.
     *
     * @param assetId    the asset ID (used as the EDC asset @id)
     * @param objectKey  the full object key / path within the bucket
     * @param bucketName the MinIO bucket containing the object
     */
    public void registerAsset(String assetId, String objectKey, String bucketName) {
        if (!StringUtils.hasText(connectorProperties.getUrl())) {
            logger.debug("DALI connector URL not configured, skipping asset registration");
            return;
        }

        Map<String, String> dataAddress = new HashMap<>();
        dataAddress.put("type", "MinioFiles");
        dataAddress.put("bucketName", bucketName);
        dataAddress.put("prefix", objectKey);
        dataAddress.put("endpoint", minioProperties.getEndpoint());
        dataAddress.put("accessKey", minioProperties.getAccessKey());
        dataAddress.put("secretKey", minioProperties.getSecretKey());

        Map<String, String> context = new HashMap<>();
        context.put("@vocab", "https://w3id.org/edc/v0.0.1/ns/");

        String fileName = extractFileName(objectKey);
        Map<String, String> properties = new HashMap<>();
        properties.put("name", fileName);
        properties.put("contenttype", detectContentType(fileName));

        Map<String, Object> asset = new HashMap<>();
        asset.put("@context", context);
        asset.put("@id", assetId);
        asset.put("properties", properties);
        asset.put("dataAddress", dataAddress);

        String url = connectorProperties.getUrl() + "/management/v3/assets";

        logger.info("Registering asset '{}' (bucket: '{}') to DALI connector: {}", assetId, bucketName, url);

        try {
            restClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(asset)
                    .retrieve()
                    .toBodilessEntity();

            logger.info("✓ Asset '{}' registered to DALI connector", assetId);
        } catch (Exception e) {
            logger.warn("⚠ Failed to register asset '{}': {}", assetId, e.getMessage());
        }
    }

    private String extractFileName(String objectKey) {
        if (objectKey == null) return "unknown";
        int lastSlash = objectKey.lastIndexOf('/');
        return lastSlash >= 0 ? objectKey.substring(lastSlash + 1) : objectKey;
    }

    private String detectContentType(String fileName) {
        if (fileName == null) return "application/octet-stream";
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".csv"))  return "text/csv";
        if (lower.endsWith(".json")) return "application/json";
        if (lower.endsWith(".xml"))  return "application/xml";
        if (lower.endsWith(".pdf"))  return "application/pdf";
        if (lower.endsWith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        if (lower.endsWith(".xls"))  return "application/vnd.ms-excel";
        return "application/octet-stream";
    }
}