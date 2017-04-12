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
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.Result;
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
        this.id = id.toLowerCase(); //Marathon allows only lowercase ids.
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

    String setSystemProperty(final String propertyName, final String propertyValue) {
        return new StringBuilder().append(" -D").append(propertyName).append("=").append(propertyValue).toString();
    }

    void deleteApp(final String appID) {
        try {
            Result result = marathonClient.deleteApp(appID);
            log.debug("App: {} deleted, Deployment id is: {}", appID, result.getDeploymentId());
            waitUntilDeploymentPresent(result.getDeploymentId()).get();
        } catch (MarathonException e) {
            if (e.getStatus() == NOT_FOUND.code()) {
                log.debug("Application does not exist");
            } else {
                throw new TestFrameworkException(RequestFailed, "Marathon Exception while deleting service", e);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new TestFrameworkException(InternalError, "Exception during deleteApp", e);
        }
    }

    private boolean isDeploymentPresent(final String deployementID) {
        try {
            return marathonClient.getDeployments().stream()
                    .anyMatch(deployment -> deployment.getId().equals(deployementID));
        } catch (MarathonException e) {
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching deployemtn details of " +
                    "service", e);
        }
    }

    private CompletableFuture<Void> waitUntilDeploymentPresent(final String deploymentID) {
        return FutureHelpers.loop(() -> isDeploymentPresent(deploymentID), //condition
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(5), executorService),
                executorService);
    }
}
