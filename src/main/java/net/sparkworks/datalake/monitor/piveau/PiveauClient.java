package net.sparkworks.datalake.monitor.piveau;

import com.fasterxml.jackson.databind.JsonNode;
import net.sparkworks.datalake.monitor.config.PiveauProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

/**
 * Thin HTTP client for the Piveau Hub Repo write API ({@code PUT /datasets/{id}},
 * {@code POST /datasets/{id}/distributions}).
 *
 * <p>Authenticates with an OAuth2 client-credentials Bearer token when all three Keycloak
 * settings are present, otherwise with the static {@code X-API-Key}. Both are accepted by
 * piveau-hub-repo. Calls throw {@link org.springframework.web.client.RestClientException}
 * on a non-2xx response, so callers can treat any exception as "try again later".
 */
@Component
public class PiveauClient {

    private static final Logger logger = LoggerFactory.getLogger(PiveauClient.class);
    private static final MediaType TURTLE = MediaType.parseMediaType("text/turtle");

    private final PiveauProperties properties;
    private final RestClient restClient = RestClient.create();

    // Cached Keycloak access token, refreshed lazily on expiry rather than per request, since
    // Piveau calls come in bursts (one dataset plus many distributions).
    private volatile String cachedAccessToken;
    private volatile long tokenExpiryEpochMillis;
    private final Object tokenLock = new Object();

    public PiveauClient(PiveauProperties properties) {
        this.properties = properties;
        if (isEnabled()) {
            logger.info("Piveau client initialized — API URL: {}, auth: {}", properties.getUrl(),
                    oauth2Configured() ? "OAuth2 client-credentials (client " + properties.getKeycloakClientId() + ")"
                            : StringUtils.hasText(properties.getApiKey()) ? "API key" : "none");
        } else {
            logger.warn("piveau.url is not set — Piveau registration is disabled");
        }
    }

    public boolean isEnabled() {
        return StringUtils.hasText(properties.getUrl());
    }

    public String apiUrl() {
        return properties.getUrl();
    }

    /** Create or replace a dataset (idempotent). */
    public void putDataset(String datasetId, String catalogueId, String turtle) {
        restClient.put()
                .uri(properties.getUrl() + "/{id}?catalogue={catalogue}", datasetId, catalogueId)
                .headers(this::addAuth)
                .contentType(TURTLE)
                .accept(MediaType.APPLICATION_JSON)
                .body(turtle)
                .retrieve()
                .toBodilessEntity();
    }

    /** Whether the dataset is already registered in the catalogue (HTTP 200). */
    public boolean datasetExists(String datasetId, String catalogueId) {
        try {
            Integer status = restClient.get()
                    .uri(properties.getUrl() + "/{id}?catalogue={catalogue}", datasetId, catalogueId)
                    .headers(this::addAuth)
                    .accept(TURTLE)
                    .exchange((request, response) -> response.getStatusCode().value());
            return status != null && status == 200;
        } catch (Exception e) {
            logger.warn("Could not check whether dataset '{}' exists in Piveau: {}", datasetId, e.getMessage());
            return false;
        }
    }

    /** Add a distribution to an existing dataset. Not idempotent: Piveau mints a new one each call. */
    public void postDistribution(String datasetId, String turtle) {
        restClient.post()
                .uri(properties.getUrl() + "/{id}/distributions", datasetId)
                .headers(this::addAuth)
                .contentType(TURTLE)
                .accept(MediaType.APPLICATION_JSON)
                .body(turtle)
                .retrieve()
                .toBodilessEntity();
    }

    private boolean oauth2Configured() {
        return StringUtils.hasText(properties.getKeycloakTokenUrl())
                && StringUtils.hasText(properties.getKeycloakClientId())
                && StringUtils.hasText(properties.getKeycloakClientSecret());
    }

    private void addAuth(HttpHeaders headers) {
        if (oauth2Configured()) {
            headers.setBearerAuth(getAccessToken());
        } else if (StringUtils.hasText(properties.getApiKey())) {
            headers.set("X-API-Key", properties.getApiKey());
        }
    }

    private String getAccessToken() {
        if (cachedAccessToken != null && System.currentTimeMillis() < tokenExpiryEpochMillis) {
            return cachedAccessToken;
        }
        synchronized (tokenLock) {
            if (cachedAccessToken != null && System.currentTimeMillis() < tokenExpiryEpochMillis) {
                return cachedAccessToken;
            }
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("grant_type", "client_credentials");
            form.add("client_id", properties.getKeycloakClientId());
            form.add("client_secret", properties.getKeycloakClientSecret());

            JsonNode json = restClient.post()
                    .uri(properties.getKeycloakTokenUrl())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(form)
                    .retrieve()
                    .body(JsonNode.class);

            String accessToken = json != null ? json.path("access_token").asText(null) : null;
            if (!StringUtils.hasText(accessToken)) {
                throw new IllegalStateException("Keycloak token response did not contain an access_token");
            }
            long expiresInSeconds = json.path("expires_in").asLong(60);
            // Refresh 30s ahead of actual expiry so an in-flight request never races a stale token.
            tokenExpiryEpochMillis = System.currentTimeMillis() + Math.max(expiresInSeconds - 30, 5) * 1000;
            cachedAccessToken = accessToken;
            logger.info("Obtained Keycloak access token (expires in {}s)", expiresInSeconds);
            return accessToken;
        }
    }
}
