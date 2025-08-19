package com.slack.astra.chunkManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

import com.slack.astra.chunk.Chunk;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.util.RuntimeHalterImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test OutOfMemoryError handling in ChunkManagerBase
 */
public class ChunkManagerOOMTest {

    private TestChunkManager chunkManager;
    
    @BeforeEach
    void setUp() {
        chunkManager = new TestChunkManager();
    }
    
    @Test
    public void testChunkQueryOOMTriggersRuntimeHalter() {
        // Arrange: Create a mock chunk that throws OOM
        Chunk<String> mockChunk = mock(Chunk.class);
        when(mockChunk.id()).thenReturn("test-chunk-1");
        when(mockChunk.containsDataInTimeRange(anyLong(), anyLong())).thenReturn(true);
        when(mockChunk.query(any(SearchQuery.class))).thenThrow(new OutOfMemoryError("Simulated OOM"));
        
        chunkManager.chunkMap.put("test-chunk-1", mockChunk);
        
        SearchQuery query = new SearchQuery("test", 0, 1000, 10, 
            "", new String[]{}, null, null);
        
        // Act & Assert: Verify OOM triggers RuntimeHalter
        try (MockedStatic<RuntimeHalterImpl> mockedHalter = mockStatic(RuntimeHalterImpl.class)) {
            RuntimeHalterImpl mockHalter = mock(RuntimeHalterImpl.class);
            mockedHalter.when(() -> new RuntimeHalterImpl()).thenReturn(mockHalter);
            
            // This should trigger the RuntimeHalter due to OOM
            assertThatThrownBy(() -> chunkManager.query(query, Duration.ofSeconds(5)))
                .isInstanceOf(OutOfMemoryError.class);
                
            // Verify handleFatal was called with OOM
            verify(mockHalter).handleFatal(any(OutOfMemoryError.class));
        }
    }
    
    @Test
    public void testNormalExceptionsStillWork() {
        // Arrange: Create a mock chunk that throws regular exception
        Chunk<String> mockChunk = mock(Chunk.class);
        when(mockChunk.id()).thenReturn("test-chunk-2");
        when(mockChunk.containsDataInTimeRange(anyLong(), anyLong())).thenReturn(true);
        when(mockChunk.query(any(SearchQuery.class))).thenThrow(new RuntimeException("Normal error"));
        
        chunkManager.chunkMap.put("test-chunk-2", mockChunk);
        
        SearchQuery query = new SearchQuery("test", 0, 1000, 10, 
            "", new String[]{}, null, null);
        
        // Act & Assert: Normal exceptions should not trigger RuntimeHalter
        try (MockedStatic<RuntimeHalterImpl> mockedHalter = mockStatic(RuntimeHalterImpl.class)) {
            RuntimeHalterImpl mockHalter = mock(RuntimeHalterImpl.class);
            mockedHalter.when(() -> new RuntimeHalterImpl()).thenReturn(mockHalter);
            
            // Should return a result with failed chunks, not crash
            SearchResult<String> result = chunkManager.query(query, Duration.ofSeconds(5));
            
            // Verify handleFatal was NOT called for normal exceptions
            verify(mockHalter, never()).handleFatal(any());
        }
    }
    
    // Test implementation of ChunkManagerBase for testing
    private static class TestChunkManager extends ChunkManagerBase<String> {
        @Override
        protected void startUp() throws Exception {
            // Test implementation
        }
        
        @Override
        protected void shutDown() throws Exception {
            // Test implementation
        }
    }
}