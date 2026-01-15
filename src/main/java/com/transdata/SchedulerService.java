package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SchedulerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerService.class);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "trans-data-scheduler");
        thread.setDaemon(true);
        return thread;
    });
    private final ExecutorService worker = Executors.newFixedThreadPool(2, r -> {
        Thread thread = new Thread(r, "trans-data-worker");
        thread.setDaemon(true);
        return thread;
    });

    private final AppConfig config;
    private final JobFactory jobFactory;
    private final UiLogSink uiLog;
    private ScheduledFuture<?> scheduledFuture;
    private Future<?> runningFuture;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public SchedulerService(AppConfig config, JobFactory jobFactory, UiLogSink uiLog) {
        this.config = config;
        this.jobFactory = jobFactory;
        this.uiLog = uiLog;
    }

    public synchronized void start() {
        if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
            return;
        }
        cancelled.set(false);
        scheduleWithInterval();
    }

    public synchronized void stop() {
        cancelled.set(true);
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (runningFuture != null) {
            runningFuture.cancel(true);
        }
        uiLog.log("INFO", "Scheduler stopped.");
    }

    public synchronized void reschedule() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (!cancelled.get()) {
            scheduleWithInterval();
        }
    }

    public synchronized void triggerOnce() {
        triggerOnceSafely();
    }

    private void triggerOnceSafely() {
        if (cancelled.get()) {
            return;
        }
        if (!config.isScheduleEnabled()) {
            uiLog.log("INFO", "Schedule disabled; skipping run.");
            return;
        }
        if (!isWithinWindow()) {
            uiLog.log("INFO", "out of window, skipped");
            return;
        }
        if (runningFuture != null && !runningFuture.isDone()) {
            uiLog.log("WARN", "Previous job still running; skipping this trigger.");
            return;
        }
        runningFuture = worker.submit(jobFactory.create(cancelled));
    }

    private void scheduleWithInterval() {
        long interval = Math.max(1, config.getIntervalSeconds());
        scheduledFuture = scheduler.scheduleWithFixedDelay(this::triggerOnceSafely, 0, interval, TimeUnit.SECONDS);
        uiLog.log("INFO", "Scheduler started with interval " + interval + " seconds.");
    }

    private boolean isWithinWindow() {
        LocalTime now = LocalTime.now();
        LocalTime start = config.getWindowStart();
        LocalTime end = config.getWindowEnd();
        if (Objects.equals(start, end)) {
            return true;
        }
        if (start.isBefore(end)) {
            return !now.isBefore(start) && !now.isAfter(end);
        }
        return !now.isBefore(start) || !now.isAfter(end);
    }

    public interface JobFactory {
        Runnable create(AtomicBoolean cancelled);
    }
}
