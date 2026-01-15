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
        uiLog.log("INFO", "调度已停止。");
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
        triggerOnceSafely(false);
    }

    private void triggerOnceSafely(boolean waitForCompletion) {
        if (cancelled.get()) {
            return;
        }
        if (!config.isScheduleEnabled()) {
            uiLog.log("INFO", "调度已关闭，跳过本次执行。");
            return;
        }
        if (!isWithinWindow()) {
            uiLog.log("INFO", "当前不在时间窗口内，已跳过。");
            return;
        }
        if (runningFuture != null && !runningFuture.isDone()) {
            uiLog.log("WARN", "上一轮任务仍在执行，已跳过本次触发。");
            return;
        }
        runningFuture = worker.submit(jobFactory.create(cancelled));
        if (waitForCompletion) {
            waitForJobCompletion();
        }
    }

    private void scheduleWithInterval() {
        long interval = Math.max(1, config.getIntervalSeconds());
        scheduledFuture = scheduler.scheduleWithFixedDelay(() -> triggerOnceSafely(true), 0, interval, TimeUnit.SECONDS);
        uiLog.log("INFO", "调度已启动，间隔 " + interval + " 秒。");
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

    private void waitForJobCompletion() {
        if (runningFuture == null) {
            return;
        }
        try {
            runningFuture.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            uiLog.log("WARN", "等待任务完成时调度线程被中断。");
        } catch (Exception ex) {
            uiLog.log("ERROR", "调度任务执行失败: " + ex.getMessage());
        }
    }

    public interface JobFactory {
        Runnable create(AtomicBoolean cancelled);
    }
}
