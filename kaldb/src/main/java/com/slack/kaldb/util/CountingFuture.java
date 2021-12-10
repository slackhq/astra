package com.slack.kaldb.util;

import org.apache.http.concurrent.FutureCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingFuture<T> implements FutureCallback<T> {
    private final AtomicInteger successCounter;

    CountingFuture(AtomicInteger successCounter) {
        this.successCounter = successCounter;
    }

    @Override
    public void completed(T t) {
        successCounter.incrementAndGet();
    }

    @Override
    public void failed(Exception e) {
        // no-op
    }

    @Override
    public void cancelled() {
        // no-op
    }
}
