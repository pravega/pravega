/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework.services;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.TestFrameworkException;
import com.emc.pravega.framework.marathon.AuthEnabledMarathonClient;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.model.v2.GetAppsResponse;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.Volume;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;
import static com.emc.pravega.framework.TestFrameworkException.Type.RequestFailed;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

/**
 * Marathon based service implementations.
 */
@Slf4j
public abstract class MarathonBasedService implements Service {

    static final boolean FORCE_IMAGE = true;
    static final int ZKSERVICE_ZKPORT = 2181;
    static final String CONTAINER_TYPE = "DOCKER";
    static final String IMAGE_PATH = System.getProperty("dockerImageRegistry");
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    static final String NETWORK_TYPE = "HOST";
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

    @Override
    public void scaleService(final int instanceCount, final boolean wait) {
        Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);
        try {
            App updatedConfig = new App();
            updatedConfig.setInstances(instanceCount);
            marathonClient.updateApp(getID(), updatedConfig, false);
            if (wait) {
                waitUntilServiceRunning().get(); // wait until scale operation is complete.
            }
        } catch (MarathonException ex) {
            if (ex.getStatus() == CONFLICT.code()) {
                log.error("Scaling operation failed as the application is locked by an ongoing deployment", ex);
                throw new TestFrameworkException(RequestFailed, "Scaling operation failed", ex);
            }
            handleMarathonException(ex);
        } catch (InterruptedException | ExecutionException ex) {
            throw new TestFrameworkException(InternalError, "Exception during scale operation", ex);
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

    Volume createVolume(final String containerPath, final String hostPath, final String mode) {
        Volume v = new Volume();
        v.setContainerPath(containerPath);
        v.setHostPath(hostPath);
        v.setMode(mode);
        return v;
    }

    HealthCheck setHealthCheck(final int gracePeriodSeconds, final String protocol,
                               final boolean ignoreHttp1xx, final int intervalSeconds, final
                               int timeoutSeconds, final int maxConsecutiveFailures) {
        HealthCheck hc = new HealthCheck();
        hc.setMaxConsecutiveFailures(maxConsecutiveFailures);
        hc.setTimeoutSeconds(timeoutSeconds);
        hc.setIntervalSeconds(intervalSeconds);
        hc.setIgnoreHttp1xx(ignoreHttp1xx);
        hc.setGracePeriodSeconds(gracePeriodSeconds);
        hc.setProtocol(protocol);
        return hc;
    }

    List<List<String>> setConstraint(final String key, final String value) {
        List<List<String>> listString = new ArrayList<>();
        listString.add(Arrays.asList(key, value));
        return listString;
    }

    void deleteApp(String appID) {
        try {
            marathonClient.deleteApp(appID);
        } catch (MarathonException e) {
            if (e.getStatus() == NOT_FOUND.code()) {
                log.debug("Application does not exist");
            } else {
                throw new TestFrameworkException(RequestFailed, "Marathon Exception while deleting service", e);
            }
        }
    }

    public void  deleteServices() {
        GetAppsResponse x = null;
        try {
            x = marathonClient.getApps();
        } catch (MarathonException e) {
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while getting service details", e);
        }
        for (int i = 0; i < x.getApps().size(); i++) {
            String id = x.getApps().get(i).getId();
            log.info("deleting app with id {}", id);
            if (!(id.startsWith("/platform") || id.startsWith("/hdfs"))) {
                deleteApp(id);
                log.info("app with id {} deleted", id);
            }
        }
    }
}
