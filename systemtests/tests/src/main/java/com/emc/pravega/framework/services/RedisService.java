/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class RedisService implements Service {
    private static final String TCP = "tcp://";

    private String id;

    private Marathon marathonClient;

    public RedisService(String id) {
        this.id = id;
        this.marathonClient = MarathonClientNautilus.getClient();
    }

    @Override
    public void start() {
        log.info("Starting service: {}", id);
        try {
            marathonClient.createApp(createRedisApp());
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    private void handleMarathonException(MarathonException e) {
        if (e.getStatus() == 404) {
            log.info("App is not running : {}", this.id);
        }
        throw new RuntimeException("Marathon Exception while fetching details of RedisService", e);
    }

    @Override
    public void stop() {
        log.info("Stopping service: {}", id);
        try {
            marathonClient.deleteApp("redisservice");
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    @Override
    public void clean() {

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

    private App createRedisApp() {
        App app = new App();
        app.setId(this.id);
        app.setCpus(1.0);
        app.setMem(1024.0);
        app.setInstances(1);
        app.setPorts(Arrays.asList(20000));
        app.setCmd("bash /opt/bootstrap.sh");
        app.setContainer(new Container());
        app.getContainer().setType("DOCKER");
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage("gpetr/redis4mesos");
        app.getContainer().getDocker().setNetwork("HOST");
        return app;
    }

    @Override
    public List<URI> getServiceDetails() {
        try {
            return marathonClient.getAppTasks(id).getTasks().stream()
                    .flatMap(task -> task.getPorts().stream()
                            .map(port -> URI.create(TCP + task.getHost() + ":" + port)))
                    .collect(Collectors.toList());
        } catch (MarathonException e) {
            throw new RuntimeException("Marathon Exception while fetching service details", e);
        }
    }
}
