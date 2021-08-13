/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.services.marathon;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.marathon.AuthEnabledMarathonClient;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonException;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.LocalVolume;
import mesosphere.marathon.client.model.v2.PortDefinition;
import mesosphere.marathon.client.model.v2.Result;
import mesosphere.marathon.client.model.v2.Volume;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;
import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

/**
 * Marathon based service implementations.
 */
@Slf4j
public abstract class MarathonBasedService implements Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final String CONTAINER_TYPE = "MESOS";
    static final String IMAGE_PATH = System.getProperty("dockerImageRegistry");
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    private static final String TCP = "tcp://";
    final String id;
    final Marathon marathonClient;

    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(3, "test");

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
            //app is not running until the desired instance count is equal to the number of task/docker containers
            // and the state of application is healthy.
            if ((app.getApp().getTasksRunning().intValue() == app.getApp().getInstances().intValue())
                    && app.getApp().getTasksRunning().intValue() == app.getApp().getTasksHealthy().intValue()) {
                log.info("App {} is running", this.id);
                return true;
            } else {
                log.info("App {} is not running", this.id);
                return false;
            }
        } catch (MarathonException ex) {
            if (ex.getStatus() == NOT_FOUND.getStatusCode()) {
                log.info("App is not running : {}", this.id);
                return false;
            }
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching service details", ex);
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
    public CompletableFuture<Void> scaleService(final int instanceCount) {
        Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);
        try {
            App updatedConfig = new App();
            updatedConfig.setInstances(instanceCount);
            marathonClient.updateApp(getID(), updatedConfig, true);

              return waitUntilServiceRunning(); // wait until scale operation is complete.

        } catch (MarathonException ex) {
            if (ex.getStatus() == CONFLICT.getStatusCode()) {
                log.error("Scaling operation failed as the application is locked by an ongoing deployment", ex);
                throw new TestFrameworkException(RequestFailed, "Scaling operation failed", ex);
            }
            handleMarathonException(ex);
        }
        return null;
    }

    void handleMarathonException(MarathonException e) {
        if (e.getStatus() == NOT_FOUND.getStatusCode()) {
            log.info("App is not running : {}", this.id);
        }
        throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching details of service", e);
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
        return Futures.loop(() -> !isRunning(), //condition
                () -> Futures.delayedFuture(Duration.ofSeconds(10), executorService),
                executorService);
    }

    Volume createVolume(final String containerPath, final String hostPath, final String mode) {
        LocalVolume v = new LocalVolume();
        v.setContainerPath(containerPath);
        v.setHostPath(hostPath);
        v.setMode(mode);
        return v;
    }

    HealthCheck setHealthCheck(final int gracePeriodSeconds, final String protocol,
                               final boolean ignoreHttp1xx, final int intervalSeconds, final
                               int timeoutSeconds, final int maxConsecutiveFailures, final int port) {
        HealthCheck hc = setHealthCheck(gracePeriodSeconds, protocol, ignoreHttp1xx, intervalSeconds, timeoutSeconds,
                maxConsecutiveFailures);
        hc.setPort(port);
        return hc;
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

    String buildSystemProperty(final String propertyName, final String propertyValue) {
        return new StringBuilder().append(" -D").append(propertyName).append("=").append(propertyValue).toString();
    }

    void deleteApp(final String appID) {
        try {
            Result result = marathonClient.deleteApp(appID);
            log.info("App: {} deleted, Deployment id is: {}", appID, result.getDeploymentId());
            waitUntilDeploymentPresent(result.getDeploymentId()).get();
        } catch (MarathonException e) {
            if (e.getStatus() == NOT_FOUND.getStatusCode()) {
                log.debug("Application does not exist");
            } else {
                throw new TestFrameworkException(RequestFailed, "Marathon Exception while deleting service", e);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new TestFrameworkException(InternalError, "Exception during deleteApp", e);
        }
    }

    private boolean isDeploymentPresent(final String deploymentID) {
        try {
            return marathonClient.getDeployments().stream()
                    .anyMatch(deployment -> deployment.getId().equals(deploymentID));
        } catch (MarathonException e) {
            throw new TestFrameworkException(RequestFailed, "Marathon Exception while fetching deployment details of " +
                    "service", e);
        }
    }

    private CompletableFuture<Void> waitUntilDeploymentPresent(final String deploymentID) {
        return Futures.loop(() -> isDeploymentPresent(deploymentID), //condition
                () -> Futures.delayedFuture(Duration.ofSeconds(5), executorService),
                executorService);
    }

    PortDefinition createPortDefinition(int port) {
        PortDefinition pd = new PortDefinition();
        pd.setPort(port);
        pd.setProtocol("tcp");
        return pd;
    }
}
