package net.sparkworks.datalake.monitor.piveau;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DcatTurtleBuilderTest {

    private static final String API = "https://dataspace.6gdali.eu/datasets";

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void datasetFromMinimalMetadataUsesDefaultsAndDirectoryName() throws Exception {
        DatasetMetadata md = mapper.readValue("{\"issued\":\"2025-11-24\"}", DatasetMetadata.class);
        md.setDatasetId("6g-dali-staging-eur-exp-1");

        String ttl = DcatTurtleBuilder.buildDataset(API, "6g-dali-staging-eur-exp-1", md, "2025-11-24", "2025-11-24");

        assertTrue(ttl.contains("<" + API + "/6g-dali-staging-eur-exp-1>"));
        assertTrue(ttl.contains("dct:title                 \"6g-dali-staging-eur-exp-1\"@en"));
        assertTrue(ttl.contains("dct:identifier            \"6g-dali-staging-eur-exp-1\""));
        assertTrue(ttl.contains("authority/language/ENG>"));
        assertTrue(ttl.contains("authority/access-right/PUBLIC>"));
        assertTrue(ttl.contains("dali:gdprCompliant        true"));
        assertTrue(ttl.contains("gax:containsPII           false"));
        assertTrue(ttl.contains("dali:snsProjectName       \"6G-DALI\""));
        assertFalse(ttl.contains("dali:testbedContext"));
        assertTrue(ttl.trim().endsWith("."), "subject block must be closed with a full stop");
    }

    @Test
    void datasetMapsTestbedContextAndAgents() throws Exception {
        DatasetMetadata md = mapper.readValue("""
                {"title":"KPI \\"set\\"","creator_name":"EURECOM 5G Lab","creator_kind":"Organization",
                 "creator_orcid":"0000-0002-1825-0097","keywords":["5G","RAN"],"produced_by":"https://dali-project.eu/participant/eurecom",
                 "testbed_context":{"ran_frequency_band":"n78","ran_bandwidth_mhz":100,"compute_gpu_use":false,
                                    "measurement_tool":["iperf3"," "]}}
                """, DatasetMetadata.class);
        md.setDatasetId("exp");

        String ttl = DcatTurtleBuilder.buildDataset(API, "exp", md, "2025-11-24", "2025-11-24");

        assertTrue(ttl.contains("\"KPI \\\"set\\\"\"@en"), "quotes in values are escaped");
        assertTrue(ttl.contains("a foaf:Organization ; foaf:name \"EURECOM 5G Lab\""));
        assertTrue(ttl.contains("schema:identifier <https://orcid.org/0000-0002-1825-0097>"));
        assertTrue(ttl.contains("dcat:keyword              \"5G\", \"RAN\""));
        assertTrue(ttl.contains("gax:producedBy            <https://dali-project.eu/participant/eurecom>"));
        assertTrue(ttl.contains("dali:ranFrequencyBand  \"n78\""), "a single band string is accepted as a list");
        assertTrue(ttl.contains("dali:ranBandwidthMHz      100"));
        assertTrue(ttl.contains("dali:computeGpuUse        false^^xsd:boolean"));
        assertTrue(ttl.contains("dali:measurementTool  \"iperf3\""));
        assertEquals(1, ttl.split("dali:measurementTool", -1).length - 1, "blank list entries are skipped");
    }

    @Test
    void distributionCarriesAssetIdUrlsAndColumns() {
        String ttl = DcatTurtleBuilder.buildDistribution(API, "exp", DcatTurtleBuilder.distributionId("exp-Data File.csv"),
                "Data File", "Data File.csv", "https://edc.dataspace.6gdali.eu",
                "http://lake:9000/bucket/exp/Data File.csv", List.of("timestamp", "value"), "iperf3",
                "https://creativecommons.org/licenses/by/4.0/", "2025-11-24");

        assertTrue(ttl.contains("<" + API + "/exp/distributions/exp-data-file>"));
        assertTrue(ttl.contains("dcat:accessURL          <https://edc.dataspace.6gdali.eu>"));
        assertTrue(ttl.contains("dali:assetId            \"Data File\""));
        assertTrue(ttl.contains("dct:format              \"CSV\""));
        assertTrue(ttl.contains("dcat:mediaType          \"text/csv\""));
        assertTrue(ttl.contains("schema:variableMeasured \"timestamp\", \"value\""));
        assertTrue(ttl.contains("schema:measurementTechnique \"iperf3\"@en"));
        assertTrue(ttl.trim().endsWith("."));
    }

    @Test
    void distributionOmitsOptionalParts() {
        String ttl = DcatTurtleBuilder.buildDistribution(API, "exp", "exp-a", "a", "a.csv", null, null, null, null,
                "https://creativecommons.org/licenses/by/4.0/", "2025-11-24");

        assertFalse(ttl.contains("dcat:accessURL"));
        assertFalse(ttl.contains("dcat:downloadURL"));
        assertFalse(ttl.contains("schema:variableMeasured"));
        assertFalse(ttl.contains("schema:measurementTechnique"));
    }
}
