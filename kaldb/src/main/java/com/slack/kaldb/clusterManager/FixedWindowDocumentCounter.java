package com.slack.kaldb.clusterManager;

import com.google.common.collect.EvictingQueue;
import com.google.common.util.concurrent.AbstractScheduledService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FixedWindowDocumentCounter extends AbstractScheduledService {

    private final long sizeMs;
    private final long advanceMs;
    private final int totalWindows;
    private final EvictingQueue<TimeWindow> timeWindowQueue;
    private TimeWindow lastTimeWindow;

    /**
     *
     * @param sizeMs total window to aggregate over
     * @param advanceMs e
     */
    public FixedWindowDocumentCounter(final long sizeMs, final long advanceMs) {
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
        this.totalWindows = Long.valueOf(sizeMs / advanceMs).intValue();
        this.timeWindowQueue = EvictingQueue.create(totalWindows);

        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (sizeMs) must be larger than zero.");
        }

        if (advanceMs <= 0 || advanceMs > sizeMs) {
            throw new IllegalArgumentException(String.format("Window advancement interval should be more than zero " +
                    "and less than window duration which is %d ms, but given advancement interval is: %d ms", sizeMs, advanceMs));
        }
        long currentTime = System.currentTimeMillis();
        TimeWindow timeWindow = new TimeWindow(currentTime, currentTime + advanceMs);
        lastTimeWindow = timeWindow;
        timeWindowQueue.add(timeWindow);
    }

    public static void main(String args[]) throws Exception {
        FixedWindowDocumentCounter fixedWindowDocumentCounter
                = new FixedWindowDocumentCounter(500, 100);
        fixedWindowDocumentCounter.startAsync();
        while (!fixedWindowDocumentCounter.isRunning()) {
            Thread.sleep(100);
        }
        for (int i=0; i<10; i++) {
            fixedWindowDocumentCounter.addCount();
            Thread.sleep(100);
        }
        fixedWindowDocumentCounter.stopAsync();
    }

    protected void addCount() {
        lastTimeWindow.count.addAndGet(1);
    }

    protected long getCount() {
        AtomicLong count = new AtomicLong();
        timeWindowQueue.forEach(timeWindow -> count.addAndGet(timeWindow.count.get()));
        return count.get();
    }

    @Override
    protected void runOneIteration() throws Exception {
        System.out.println("lastTimeWindow start=" + lastTimeWindow.startDurationMS + " end=" + lastTimeWindow.endDurationMs + " total=" + lastTimeWindow.count.get());
        TimeWindow timeWindow = new TimeWindow(lastTimeWindow.endDurationMs, lastTimeWindow.endDurationMs + advanceMs);
        lastTimeWindow = timeWindow;
        timeWindowQueue.add(timeWindow);
    }

    @Override
    protected Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedRateSchedule(
                0,
                advanceMs,
                TimeUnit.MILLISECONDS);
    }

    static class TimeWindow {

        public long startDurationMS;
        public long endDurationMs;
        public AtomicLong count;

        /**
         *
         * @param startDurationMS inclusive
         * @param endDurationMs exclusive
         */
        public TimeWindow(long startDurationMS, long endDurationMs) {
            this.startDurationMS = startDurationMS;
            this.endDurationMs = endDurationMs;
            count = new AtomicLong();
        }
    }

}
