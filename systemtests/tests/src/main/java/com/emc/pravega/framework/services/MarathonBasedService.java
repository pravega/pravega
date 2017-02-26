/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework.services;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.TestFrameworkException;
import com.emc.pravega.framework.marathon.AuthEnabledMarathonClient;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.TestFrameworkException.Type.RequestFailed;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

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
        this.marathonClient = AuthEnabledMarathonClient.getClient();
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
            if (ex.getStatus() == NOT_FOUND.code()) {
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
        if (e.getStatus() == NOT_FOUND.code()) {
            log.info("App is not running : {}", this.id);
        }
        throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching details of service", e);
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
        return FutureHelpers.loop(() -> !isRunning(), //condition
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(5), executorService),
                executorService);
    }

    @Override
    public boolean isStaged() {

        try {
            GetAppResponse app = marathonClient.getApp(this.id);
            log.debug("App Details: {}", app);

            if (app.getApp().getTasksStaged() != 0) {
                log.info("App {} is staged", this.id);
                return true;
            } else {
                log.info("App {} is not staged yet", this.id);
                return false;
            }
        } catch (MarathonException ex) {
            if (ex.getStatus() == NOT_FOUND.code()) {
                log.info("App is not staged : {}", this.id);
                return false;
            }
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while " +
                    "fetching service details", ex);
        }

    }
}
