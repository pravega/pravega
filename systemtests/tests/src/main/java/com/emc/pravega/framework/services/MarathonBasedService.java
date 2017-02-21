/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.framework.services;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.TestFrameworkException;
import com.emc.pravega.framework.marathon.MarathonClientNautilus;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.TestFrameworkException.Type.RequestFailed;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Marathon based service implementations.
 */
@Slf4j
public abstract class MarathonBasedService implements Service {
    private static final String TCP = "tcp://";

    final String id;
    final Marathon marathonClient;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    MarathonBasedService(final String id) {
        this.id = id;
        this.marathonClient = MarathonClientNautilus.getClient();
    }

    @Override
    public String getID() {
        return this.id;
    }

    @Override
    public boolean isRunning() {
        try {
            GetAppResponse app = marathonClient.getApp(this.id);
            log.debug("App Details: {}", app);

            if (app.getApp().getTasksStaged() == 0 && app.getApp().getTasksRunning() != 0) {
                log.info("App {} is running", this.id);
                return true;
            } else {
                log.info("App {} is getting staged or no tasks are running", this.id);
                return false;
            }
        } catch (MarathonException ex) {
            if (ex.getStatus() == NOT_FOUND.getStatusCode()) {
                log.info("App is not running : {}", this.id);
                return false;
            }
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while " +
                    "fetching service details", ex);
        }
    }

    @Override
    public List<URI> getServiceDetails() {
        try {
            return marathonClient.getAppTasks(getID()).getTasks().stream()
                    .flatMap(task -> task.getPorts().stream()
                            .map(port -> URI.create(TCP + task.getHost() + ":" + port)))
                    .collect(Collectors.toList());
        } catch (MarathonException ex) {
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching service details", ex);
        }
    }

    void handleMarathonException(MarathonException e) {
        if (e.getStatus() == NOT_FOUND.getStatusCode()) {
            log.info("App is not running : {}", this.id);
        }
        throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching details of RedisService", e);
    }

    void waitUntilServiceRunning() {
        AtomicBoolean mustWait = new AtomicBoolean(true);
        try {
            FutureHelpers.loop(mustWait::get, //condition
                    () -> CompletableFuture.runAsync(() -> mustWait.set(!isRunning()))
                            .thenCompose(v -> FutureHelpers.delayedFuture(mustWait.get() ? Duration.ofSeconds(5) :
                                    Duration.ZERO, executorService)
                            ), executorService

            ).get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Error while waiting for " +
                    "service to start", ex);
        }
    }
}
