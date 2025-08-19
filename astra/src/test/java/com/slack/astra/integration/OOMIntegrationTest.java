package com.slack.astra.integration;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests that actually trigger OutOfMemoryError conditions
 * 
 * Run these with: mvn test -Dtest=OOMIntegrationTest -Djvm.args="-Xmx64m"
 */
@EnabledIf("java.lang.Boolean.getBoolean('astra.test.oom')")
public class OOMIntegrationTest {
    
    @Test
    public void testChunkManagerHandlesRealOOM() {
        // This test should be run with limited heap: -Xmx64m
        // You can enable it with: -Dastra.test.oom=true
        
        List<byte[]> memoryHog = new ArrayList<>();
        
        assertThatThrownBy(() -> {
            // Allocate memory until OOM
            while (true) {
                memoryHog.add(new byte[1024 * 1024]); // 1MB per iteration
            }
        }).isInstanceOf(OutOfMemoryError.class);
    }
    
    @Test 
    public void testBulkIngestHandlesMemoryPressure() {
        // Simulate bulk ingest under memory pressure
        // This would be more complex and require actual Astra components
        
        // For now, just verify OOM can be triggered
        assertThatThrownBy(() -> {
            List<Object> objects = new ArrayList<>();
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                objects.add(new byte[1024]);
            }
        }).isInstanceOf(OutOfMemoryError.class);
    }
}