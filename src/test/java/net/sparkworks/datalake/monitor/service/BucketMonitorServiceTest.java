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
}
