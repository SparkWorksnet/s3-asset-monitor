# s3-asset-monitor

Watches the 6G-DALI data lake (S3-compatible storage) and, for every new file, makes it available in the
data space:

| New file | What the monitor does |
|---|---|
| `<experiment>/metadata.json` | Registers the **dataset** in the Piveau catalogue (DCAT-AP 3.0 + GAIA-X + `dali:` extensions, per the Metadata Application Profile). |
| `<experiment>/<file>.csv` (see `monitor.file-extensions`) | Registers the file as an **EDC asset** on the DALI connector, then adds it to its dataset in Piveau as a **distribution** (`dali:assetId` = the asset id, `dcat:accessURL` = the connector, `dcat:downloadURL` = the S3 object). |

The bucket is the Piveau **catalogue id** and the directory (`<experiment>`) is the **dataset id**.
Testbed connectors only write files into the lake; they no longer talk to Piveau.

## How it works

- Objects under a path segment starting with a dot (e.g. `.datasets/`, `.files/`, the testbed
  connector's UUID mappings) are bookkeeping and are ignored, even if they end in `.csv`.
- On startup every existing matching file is marked as seen and skipped. Only files that appear
  afterwards are handled.
- Each poll (`monitor.poll-interval-seconds`) handles new files, `metadata.json` first, so a
  dataset exists before its files are added to it.
- A file whose dataset is not in Piveau yet, or whose registration fails, is retried on the
  next poll, up to `piveau.max-attempts` times (default 20). After that it is skipped until the
  monitor restarts.
- Distribution columns, measurement technique and licence come from the dataset's
  `metadata.json`. After a restart it is re-read from the lake.
- `metadata.json` is the format in `6gdali-metadata-application-profile/Testbed-Metadata-JSON-guide.md`.
- Registering a dataset is an idempotent `PUT`. Creating a distribution is not, so a file is
  only registered once.

## Configuration

| Environment variable | Default | Meaning |
|---|---|---|
| `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` | | Data lake connection (any S3-compatible store) |
| `MONITOR_BUCKETS` | `datalake` | Buckets to watch (comma-separated) |
| `MONITOR_POLL_INTERVAL_SECONDS` | `30` | Poll interval |
| `MONITOR_METADATA_FILE_NAME` | `metadata.json` | Per-dataset metadata file name (case-insensitive) |
| `DALI_CONNECTOR_URL` | | Connector management API base URL. Empty disables EDC asset registration. |
| `DALI_CONNECTOR_ACCESS_URL` | `DALI_CONNECTOR_URL` | Public connector URL for `dcat:accessURL` |
| `PIVEAU_URL` | | Piveau Hub Repo dataset API, e.g. `https://dataspace.6gdali.eu/datasets`. Empty disables catalogue registration. |
| `PIVEAU_API_KEY` | | Static `X-API-Key` (used when OAuth2 is not configured) |
| `PIVEAU_KEYCLOAK_TOKEN_URL`, `PIVEAU_KEYCLOAK_CLIENT_ID`, `PIVEAU_KEYCLOAK_CLIENT_SECRET` | | OAuth2 client-credentials. All three enable it and take precedence over the API key. |
| `PIVEAU_MAX_ATTEMPTS` | `20` | Polls a file is retried |

Prometheus metrics are at `/actuator/prometheus` (`s3_monitor_*`).

## Authenticating against Piveau Hub Repo

The Piveau write API accepts either a static API key or an OAuth2 Bearer token from a Keycloak
confidential client's service account, scoped to specific catalogues. Do this once per catalogue:

1. **Create a confidential client** in the realm Piveau is configured against: *Clients → Create
   client*, type `OpenID Connect`, e.g. `dali-s3-asset-monitor`. Enable **Client authentication**
   and **Service accounts roles**, and leave the other flows off. Copy the secret from the
   **Credentials** tab.
2. **Grant access to the catalogue.** Piveau models per-catalogue write access as a Keycloak
   group named after the catalogue. Add the client's service account user
   (`service-account-<client-id>`) to that group. One group per watched bucket.
3. **Token URL** is `<keycloak-server-url>/realms/<realm>/protocol/openid-connect/token`, using the
   `serverUrl` and `realm` from the Piveau deployment's `PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA`.
4. **Verify**:
   ```bash
   curl -s -X POST "<token url>" -d grant_type=client_credentials \
     -d client_id=<client id> -d client_secret=<client secret>
   ```
   returns an `access_token`. A `PUT <PIVEAU_URL>/<id>?catalogue=<bucket>` with it should succeed
   for catalogues the service account was added to, and return 403 for others.

Rotate by regenerating the secret and updating `PIVEAU_KEYCLOAK_CLIENT_SECRET`. Revoke by
removing the service account from the catalogue's group.

## Possible improvements

- **Show the original file name in Piveau.** Testbed connectors store each data file in the lake
  as `<dataset-uuid>/<random-uuid>.csv` and keep the file's original name as the object's
  `original-name` user metadata (URL-encoded). The monitor does not use it, so a distribution's
  `dct:title` is the UUID file name. To show the original name, call `statObject` for each new
  file in `PiveauRegistrationService.registerDistribution`, URL-decode
  `userMetadata().get("original-name")` and use it for `dct:title` and `dct:description`, falling
  back to the object's file name when the metadata is missing (older files, or a store that does
  not keep user metadata). The URLs, `dali:assetId`, format and media type do not change. Costs
  one extra request per new file.
