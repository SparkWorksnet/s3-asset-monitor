package net.sparkworks.datalake.monitor.service;

import net.sparkworks.datalake.monitor.config.DaliConnectorProperties;
import net.sparkworks.datalake.monitor.config.S3Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Registers files discovered in S3 as assets in the DALI EDC connector Management API —
 * mirrors dataops-orchestrator's edc_client.py (the "upload form" path's own asset
 * registration step), so assets from either source share the same connector-side shape
 * and are equally negotiable: same dataAddress type, and the same shared
 * policy/contract-definition pair (created idempotently here too, not assumed to already
 * exist), rather than only ever POSTing the asset itself.
 */
@Service
public class EdcAssetRegistrationService {

    private static final Logger logger = LoggerFactory.getLogger(EdcAssetRegistrationService.class);

    // Shared with dataops-orchestrator/edc_client.py — one global policy/contract-definition
    // pair (assetsSelector: [] matches every asset on the connector), created once
    // (idempotently — a 409 because it already exists is treated as success) and reused by
    // every asset from either registration path.
    private static final String POLICY_ID = "dali-no-constraint-policy";
    private static final String CONTRACT_DEFINITION_ID = "dali-contract-definition";

    private final DaliConnectorProperties connectorProperties;
    private final S3Properties s3Properties;
    private final RestClient restClient;

    public EdcAssetRegistrationService(DaliConnectorProperties connectorProperties,
                                       S3Properties s3Properties) {
        this.connectorProperties = connectorProperties;
        this.s3Properties = s3Properties;
        this.restClient = RestClient.create();
        logger.info("EdcAssetRegistrationService initialized — connector URL: {}", connectorProperties.getUrl());
    }

    /**
     * Register an S3 object as an asset in the DALI EDC connector.
     *
     * @param assetId    the asset ID (used as the EDC asset @id)
     * @param objectKey  the full object key / path within the bucket
     * @param bucketName the S3 bucket containing the object
     */
    public void registerAsset(String assetId, String objectKey, String bucketName) {
        if (!StringUtils.hasText(connectorProperties.getUrl())) {
            logger.debug("DALI connector URL not configured, skipping asset registration");
            return;
        }

        ensurePolicyAndContractDefinition();

        Map<String, String> dataAddress = new HashMap<>();
        dataAddress.put("type", "MinioAsset");
        dataAddress.put("bucketName", bucketName);
        dataAddress.put("prefix", objectKey);
        dataAddress.put("endpoint", s3Properties.getEndpoint());
        dataAddress.put("accessKey", s3Properties.getAccessKey());
        dataAddress.put("secretKey", s3Properties.getSecretKey());

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
        } catch (HttpClientErrorException.Conflict e) {
            logger.info("Asset '{}' already registered on DALI connector", assetId);
        } catch (Exception e) {
            logger.warn("⚠ Failed to register asset '{}': {}", assetId, e.getMessage());
        }
    }

    /**
     * Idempotently create the shared policy + contract-definition every registered asset
     * relies on to actually be negotiable — safe to call before every registration; a 409
     * (already exists) is not an error. Best-effort like registerAsset itself: a failure here
     * is logged but doesn't stop the asset registration attempt that follows.
     */
    private void ensurePolicyAndContractDefinition() {
        Map<String, String> context = new HashMap<>();
        context.put("@vocab", "https://w3id.org/edc/v0.0.1/ns/");

        Map<String, Object> policy = new HashMap<>();
        Map<String, String> policyBody = new HashMap<>();
        policyBody.put("@context", "http://www.w3.org/ns/odrl.jsonld");
        policyBody.put("@type", "Set");
        policy.put("@context", context);
        policy.put("@id", POLICY_ID);
        policy.put("policy", policyBody);

        try {
            restClient.post()
                    .uri(connectorProperties.getUrl() + "/management/v3/policydefinitions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(policy)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException.Conflict e) {
            // already exists — fine
        } catch (Exception e) {
            logger.warn("⚠ Failed to ensure policy definition '{}': {}", POLICY_ID, e.getMessage());
        }

        Map<String, Object> contractDefinition = new HashMap<>();
        contractDefinition.put("@context", context);
        contractDefinition.put("@id", CONTRACT_DEFINITION_ID);
        contractDefinition.put("accessPolicyId", POLICY_ID);
        contractDefinition.put("contractPolicyId", POLICY_ID);
        contractDefinition.put("assetsSelector", new Object[0]);

        try {
            restClient.post()
                    .uri(connectorProperties.getUrl() + "/management/v3/contractdefinitions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(contractDefinition)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException.Conflict e) {
            // already exists — fine
        } catch (Exception e) {
            logger.warn("⚠ Failed to ensure contract definition '{}': {}", CONTRACT_DEFINITION_ID, e.getMessage());
        }
    }

    private String extractFileName(String objectKey) {
        if (objectKey == null) return "unknown";
        int lastSlash = objectKey.lastIndexOf('/');
        return lastSlash >= 0 ? objectKey.substring(lastSlash + 1) : objectKey;
    }

    /**
     * The EDC asset @id to register an object under: just its filename, extension removed
     * (e.g. "some-folder/file.csv" -> "file") — not the full object key, and not the
     * extension, which is carried instead in dataAddress.prefix and the contenttype
     * property. Note this means two files with the same basename in different folders of
     * the same bucket would collide on the same asset id.
     */
    public String deriveAssetId(String objectKey) {
        String fileName = extractFileName(objectKey);
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(0, lastDot) : fileName;
    }

    private String detectContentType(String fileName) {
        if (fileName == null) return "application/octet-stream";
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".csv"))     return "text/csv";
        if (lower.endsWith(".tsv"))     return "text/tab-separated-values";
        if (lower.endsWith(".jsonl"))   return "application/jsonl";
        if (lower.endsWith(".ndjson"))  return "application/x-ndjson";
        if (lower.endsWith(".json"))    return "application/json";
        if (lower.endsWith(".jsonld"))  return "application/ld+json";
        if (lower.endsWith(".txt"))     return "text/plain";
        if (lower.endsWith(".xml"))     return "application/xml";
        if (lower.endsWith(".parquet")) return "application/parquet";
        if (lower.endsWith(".pdf"))     return "application/pdf";
        if (lower.endsWith(".xlsx"))    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        if (lower.endsWith(".xls"))     return "application/vnd.ms-excel";
        return "application/octet-stream";
    }
}