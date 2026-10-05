package net.sparkworks.datalake.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the Piveau Hub Repo the monitor registers datasets and
 * distributions with.
 *
 * <p>Auth is either an OAuth2 client-credentials Bearer token (all three {@code keycloak-*}
 * values set) or, as a fallback, the static {@code api-key}.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "piveau")
public class PiveauProperties {

    /**
     * Base URL of the Piveau Hub Repo dataset API, e.g. {@code https://dataspace.6gdali.eu/datasets}.
     * When empty, Piveau registration is disabled.
     */
    private String url;

    /** Static {@code X-API-Key}, used when OAuth2 is not configured. */
    private String apiKey;

    /** Full Keycloak token endpoint, {@code https://<host>/realms/<realm>/protocol/openid-connect/token}. */
    private String keycloakTokenUrl;

    /** Confidential client id (service account enabled). */
    private String keycloakClientId;

    /** Confidential client secret. */
    private String keycloakClientSecret;

    /**
     * How many polls a file's Piveau registration is attempted before the monitor gives up on it
     * (until restart). A CSV waits here for its dataset's {@code metadata.json} to be registered.
     */
    private int maxAttempts = 20;
}
