/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.framework.tasks;

import io.pravega.common.concurrent.FutureHelpers;
import feign.Response;
import io.pravega.framework.TestFrameworkException;
import io.pravega.framework.metronome.AuthEnabledMetronomeClient;
import io.pravega.framework.metronome.Metronome;
import io.pravega.framework.metronome.MetronomeException;
import io.pravega.framework.metronome.model.v1.Job;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.pravega.framework.TestFrameworkException.*;

/**
 * Metronome based task implementation.
 */
@Slf4j
public abstract class MetronomeBasedTask implements Task {

    final String id;
    final Metronome metronomeClient;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    MetronomeBasedTask(final String id) {
        this.id = id.toLowerCase(); //Metronome does not allow Upper case in ids.
        this.metronomeClient = AuthEnabledMetronomeClient.getClient();
    }

    @Override
    public String getID() {
        return this.id;
    }

    private boolean isTaskRunning(final String jobId) {
        Job jobStatus = metronomeClient.getJob(jobId);
        boolean isRunning = false;
        if (jobStatus.getHistory() == null) {
            isRunning = true;
        } else if ((jobStatus.getHistory().getSuccessCount() == 0) && (jobStatus.getHistory().getFailureCount() == 0)) {
            isRunning = true;
        }
        return isRunning;
    }

    protected CompletableFuture<Void> waitForTaskCompletion(final String jobId) {
        return FutureHelpers.loop(() -> isTaskRunning(jobId),
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(3), executorService),
                executorService);
    }

    protected void deleteTask(final String jobId) {
        try {
            metronomeClient.deleteJob(jobId);
        } catch (MetronomeException e) {
            throw new TestFrameworkException(Type.RequestFailed, "Error while deleting the " +
                    "test run job", e);
        }
    }

    protected CompletableFuture<Void> startTaskExecution(final Job job) {
        return CompletableFuture.<Void>runAsync(() -> {
            metronomeClient.createJob(job);
            Response response = metronomeClient.triggerJobRun(job.getId());
            if (response.status() != CREATED.code()) {
                throw new TestFrameworkException(Type.ConnectionFailed, "Error while starting " +
                        "Task : " + job.getId());
            }
        }).thenCompose(v2 -> waitForTaskCompletion(job.getId()))
                .<Void>thenApply(v1 -> {
                    if (metronomeClient.getJob(job.getId()).getHistory().getFailureCount() != 0) {
                        throw new TestFrameworkException(Type.InternalError, "Error while " +
                                "executing task" + job.getId());
                    }
                    return null;
                }).whenComplete((v, ex) -> {
                    deleteTask(job.getId()); //deletejob once execution is complete.
                    if (ex != null) {
                        log.error("Error while executing task. TaskId: {}", job.getId());
                    }
                });
    }
}
