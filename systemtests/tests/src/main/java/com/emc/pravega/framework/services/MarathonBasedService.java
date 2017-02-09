/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.framework.services;

import com.emc.pravega.framework.marathon.MarathonClientNautilus;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Marathon based service implementations.
 */
@Slf4j
public abstract class MarathonBasedService implements Service {
    private static final String TCP = "tcp://";

    protected String id;
    protected Marathon marathonClient;

    public MarathonBasedService(final String id) {
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
        } catch (MarathonException e) {
            if (e.getStatus() == 404) {
                log.info("App is not running : {}", this.id);
                return false;
            }
            throw new RuntimeException("Marathon Exception while fetching service details", e);
        }
    }

    @Override
    public List<URI> getServiceDetails() {
        try {
            return marathonClient.getAppTasks(getID()).getTasks().stream()
                    .flatMap(task -> task.getPorts().stream()
                            .map(port -> URI.create(TCP + task.getHost() + ":" + port)))
                    .collect(Collectors.toList());
        } catch (MarathonException e) {
            throw new RuntimeException("Marathon Exception while fetching service details", e);
        }
    }

    protected void handleMarathonException(MarathonException e) {
        if (e.getStatus() == 404) {
            log.info("App is not running : {}", this.id);
        }
        throw new RuntimeException("Marathon Exception while fetching details of RedisService", e);
    }

    public void waitUntilServiceRunning() {
        try {
            while (!isRunning()) {
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while waiting for service to start", e);
        }
    }
}
