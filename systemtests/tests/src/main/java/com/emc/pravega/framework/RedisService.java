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

package com.emc.pravega.framework;

import com.emc.pravega.framework.marathon.MarathonClientSingleton;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.model.v2.GetServerInfoResponse;
import mesosphere.marathon.client.model.v2.Task;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class RedisService implements Service {
    public static final String TCP = "tcp://";
    private String id;

    private Marathon marathonClient;

    public RedisService(String id) {
        this.id = id;
        this.marathonClient = MarathonClientSingleton.INSTANCE.getClient();
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
        Marathon marathon = MarathonClientSingleton.INSTANCE.getClient();
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
            log.debug(app.toString());

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
            throw new RuntimeException("Marathon Exception while deploying RedisService", e);
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

    public void test() {
        GetServerInfoResponse info = null;
        try {
            info = marathonClient.getServerInfo();
            System.out.println(info);
            Collection<Task> tasks = marathonClient.getAppTasks(id).getTasks();
            tasks.stream().forEach(task -> {
                System.out.println(task);
            });
        } catch (MarathonException e) {
            throw new RuntimeException("Marathon Exception while deploying RedisService", e);
        }

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
