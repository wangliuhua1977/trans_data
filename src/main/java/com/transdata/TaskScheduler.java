package com.transdata;

import java.time.LocalTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskScheduler {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "trans-data-task-scheduler");
        thread.setDaemon(true);
        return thread;
    });
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "trans-data-task-worker");
        thread.setDaemon(true);
        return thread;
    });

    private final TaskDefinition task;
    private final TaskJobFactory jobFactory;
    private final UiLogSink uiLog;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private ScheduledFuture<?> scheduledFuture;

    public TaskScheduler(TaskDefinition task, TaskJobFactory jobFactory, UiLogSink uiLog) {
        this.task = task;
        this.jobFactory = jobFactory;
        this.uiLog = uiLog;
    }

    public synchronized void start() {
        if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
            return;
        }
        cancelled.set(false);
        scheduleNext(0);
    }

    public synchronized void stop() {
        cancelled.set(true);
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        uiLog.log("INFO", "调度已停止。");
    }

    public synchronized void triggerOnce() {
        if (cancelled.get()) {
            return;
        }
        worker.submit(() -> executeRun(true));
    }

    public synchronized void reschedule() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (!cancelled.get()) {
            scheduleNext(0);
        }
    }

    private void scheduleNext(long delaySeconds) {
        scheduledFuture = scheduler.schedule(() -> worker.submit(() -> executeRun(false)), delaySeconds, TimeUnit.SECONDS);
    }

    private void executeRun(boolean manual) {
        if (cancelled.get()) {
            return;
        }
        if (!task.isEnabled()) {
            uiLog.log("INFO", "任务已停用，跳过执行。任务=" + task.displayName());
            scheduleNext(30);
            return;
        }
        if (!isWithinWindow()) {
            uiLog.log("INFO", "当前不在时间窗口内，稍后再试。任务=" + task.displayName());
            scheduleNext(30);
            return;
        }
        TaskRunResult result = jobFactory.create(cancelled).run();
        if (manual) {
            return;
        }
        if (result == TaskRunResult.SUCCESS) {
            scheduleNext(Math.max(1, task.getSchedulePolicy().getIntervalSeconds()));
        } else {
            scheduleNext(15);
        }
    }

    private boolean isWithinWindow() {
        SchedulePolicy policy = task.getSchedulePolicy();
        LocalTime now = LocalTime.now();
        LocalTime start = policy.resolveWindowStart();
        LocalTime end = policy.resolveWindowEnd();
        if (Objects.equals(start, end)) {
            return true;
        }
        if (start.isBefore(end)) {
            return !now.isBefore(start) && !now.isAfter(end);
        }
        return !now.isBefore(start) || !now.isAfter(end);
    }

    public interface TaskJobFactory {
        TaskExecutable create(AtomicBoolean cancelled);
    }

    public interface TaskExecutable {
        TaskRunResult run();
    }
}
