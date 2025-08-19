package com.slack.astra.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test OutOfMemoryError handling in thread pools and background tasks
 */
public class OOMHandlingTest {

    @Test
    public void testUncaughtExceptionHandlerCatchesOOM() throws InterruptedException {
        // Arrange: Create a thread pool with OOM-aware exception handler
        try (MockedStatic<RuntimeHalterImpl> mockedHalter = mockStatic(RuntimeHalterImpl.class)) {
            RuntimeHalterImpl mockHalter = mock(RuntimeHalterImpl.class);
            mockedHalter.when(() -> new RuntimeHalterImpl()).thenReturn(mockHalter);
            
            ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                    .setUncaughtExceptionHandler((t, e) -> {
                        if (e instanceof OutOfMemoryError) {
                            new RuntimeHalterImpl().handleFatal(e);
                        }
                    })
                    .setNameFormat("test-thread-%d")
                    .build()
            );
            
            // Act: Submit a task that throws OOM
            executor.submit(() -> {
                throw new OutOfMemoryError("Test OOM in background thread");
            });
            
            // Wait for the task to execute
            Thread.sleep(100);
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            
            // Assert: Verify handleFatal was called
            verify(mockHalter, timeout(1000)).handleFatal(any(OutOfMemoryError.class));
        }
    }
    
    @Test 
    public void testUncaughtExceptionHandlerIgnoresNormalExceptions() throws InterruptedException {
        // Arrange: Create a thread pool with OOM-aware exception handler
        try (MockedStatic<RuntimeHalterImpl> mockedHalter = mockStatic(RuntimeHalterImpl.class)) {
            RuntimeHalterImpl mockHalter = mock(RuntimeHalterImpl.class);
            mockedHalter.when(() -> new RuntimeHalterImpl()).thenReturn(mockHalter);
            
            ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                    .setUncaughtExceptionHandler((t, e) -> {
                        if (e instanceof OutOfMemoryError) {
                            new RuntimeHalterImpl().handleFatal(e);
                        }
                        // Normal exceptions just get logged
                    })
                    .setNameFormat("test-thread-%d")
                    .build()
            );
            
            // Act: Submit a task that throws normal exception
            executor.submit(() -> {
                throw new RuntimeException("Normal exception");
            });
            
            // Wait for the task to execute
            Thread.sleep(100);
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            
            // Assert: Verify handleFatal was NOT called for normal exceptions
            verify(mockHalter, never()).handleFatal(any());
        }
    }
}