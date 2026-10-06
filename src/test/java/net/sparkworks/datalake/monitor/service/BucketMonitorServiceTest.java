package net.sparkworks.datalake.monitor.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BucketMonitorServiceTest {

    @Test
    void connectorMappingObjectsAreHiddenEvenWhenTheyEndInCsv() {
        assertTrue(BucketMonitorService.isHidden(".files/6g-dali-staging-kul-exp-06/amf-performance.csv"));
        assertTrue(BucketMonitorService.isHidden(".datasets/6g-dali-staging-kul-exp-06"));
        assertTrue(BucketMonitorService.isHidden("some-uuid/.hidden/file.csv"));
    }

    @Test
    void dataAndMetadataObjectsAreNotHidden() {
        assertFalse(BucketMonitorService.isHidden("0f8b1c2e-1111-4222-8333-444455556666/metadata.json"));
        assertFalse(BucketMonitorService.isHidden("0f8b1c2e-1111-4222-8333-444455556666/9a7e0000-1111-4222-8333-444455556666.csv"));
        assertFalse(BucketMonitorService.isHidden("file.with.dots.csv"));
    }

    private static final java.util.List<java.util.regex.Pattern> DEFAULT_IGNORES =
            new net.sparkworks.datalake.monitor.config.MonitorProperties().getIgnorePatterns().stream()
                    .map(java.util.regex.Pattern::compile).toList();

    @Test
    void dataOpsGeneratedArtifactsAreIgnored() {
        String dir = "8bd8e31f-be5d-40f2-bfe9-80c8449e8c4c/";
        String asset = "5953562c-69e8-47f1-8a09-350641b9855f";
        for (String suffix : new String[]{"_raw.csv", "_remediated.csv", "_soft_cleaned.csv", "_report.json",
                "_darts_linear_imputed.csv"}) {
            assertTrue(BucketMonitorService.isIgnored(dir + asset + "_20261006T131050Z" + suffix, DEFAULT_IGNORES), suffix);
        }
    }

    @Test
    void uploadedDataAndMetadataAreNotIgnored() {
        assertFalse(BucketMonitorService.isIgnored("8bd8e31f-be5d-40f2-bfe9-80c8449e8c4c/5953562c-69e8-47f1-8a09-350641b9855f.csv", DEFAULT_IGNORES));
        assertFalse(BucketMonitorService.isIgnored("8bd8e31f-be5d-40f2-bfe9-80c8449e8c4c/metadata.json", DEFAULT_IGNORES));
        assertFalse(BucketMonitorService.isIgnored("20261006T131050Z/data.csv", DEFAULT_IGNORES));
    }
}
