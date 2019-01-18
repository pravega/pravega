/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.docker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ServiceCreateResponse;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import com.spotify.docker.client.messages.swarm.TaskStatus;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import static io.pravega.test.system.framework.DockerBasedTestExecutor.DOCKER_CLIENT_PORT;
import static org.junit.Assert.assertNotNull;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.spotify.docker.client.messages.swarm.Task;

@Slf4j
public abstract class DockerBasedService implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final String IMAGE_PATH = Utils.isAwsExecution() ? "" :  System.getProperty("dockerImageRegistry") + "/";
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    static final String MASTER_IP = Utils.isAwsExecution() ? System.getProperty("awsMasterIP").trim() : System.getProperty("masterIP");
    private static final String CMD_SHELL = "CMD-SHELL"; // Docker Instruction used to run a health check command.
    final DockerClient dockerClient;
    final String serviceName;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    DockerBasedService(final String serviceName) {
        this.dockerClient = DefaultDockerClient.builder().uri("http://" + MASTER_IP + ":" + DOCKER_CLIENT_PORT).build();
        this.serviceName = serviceName;
    }

    @Override
    public String getID() {
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        String serviceId = null;
        try {
            List<Service> serviceList = Exceptions.handleInterruptedCall(
                    () -> dockerClient.listServices(criteria));
            serviceId = serviceList.get(0).id();
        } catch (DockerException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Unable to get service id", e);
        }
        return serviceId;
    }

    @Override
    public boolean isRunning() {
        boolean value = false;
        int taskRunningCount = 0;
        try {
            //list the service with filter 'serviceName'
            Service.Criteria servicecriteria = Service.Criteria.builder().serviceName(serviceName).build();
            List<Service> serviceList = Exceptions.handleInterruptedCall(
                    () -> dockerClient.listServices(servicecriteria));
            log.info("Service list size {}", serviceList.size());
            Task.Criteria taskCriteria = Task.Criteria.builder().serviceName(serviceName).build();
            List<Task> taskList = Exceptions.handleInterruptedCall(
                    () -> dockerClient.listTasks(taskCriteria));
            log.info("Task list size {}", taskList.size());
            if (!taskList.isEmpty()) {
                for (int j = 0; j < taskList.size(); j++) {
                    log.info("Task id {}", taskList.get(j).id());
                    String state = taskList.get(j).status().state();
                    log.info("Task state {}", state);
                    if (state.equals(TaskStatus.TASK_STATE_RUNNING)) {
                        taskRunningCount++;
                    }
                }
            }
            if (!serviceList.isEmpty()) {
                long replicas = Exceptions.handleInterruptedCall(
                        () -> dockerClient.inspectService(serviceList.get(0).id()).spec().mode().replicated().replicas());
                log.info("Replicas {}", replicas);
                log.info("Task running count {}", taskRunningCount);
                if (((long) taskRunningCount) == replicas) {
                    return true;
                }
            }
        } catch (DockerException e) {
            log.error("Unable to list docker services", e);
        }
        return value;
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
        log.debug("IS RUNNING {}", isRunning());
        return Futures.loop(() -> !isRunning(), //condition
                () -> Futures.delayedFuture(Duration.ofSeconds(5), executorService),
                executorService);
    }

    @Override
    public List<URI> getServiceDetails() {
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        List<URI> uriList = new ArrayList<>();
        try {
            Task.Criteria taskCriteria = Task.Criteria.builder().taskName(serviceName).build();
            List<Task> taskList = Exceptions.handleInterruptedCall(() -> dockerClient.listTasks(taskCriteria));
            log.info("Task size {}", taskList.size());

            if (!taskList.isEmpty()) {
                log.info("Network addresses {}", taskList.get(0).networkAttachments().get(0).addresses().get(0));
                List<Service> serviceList = Exceptions.handleInterruptedCall(() -> dockerClient.listServices(criteria));
                log.info("Service list size {}", serviceList.size());
                for (int i = 0; i < taskList.size(); i++) {
                    log.info("task {}", taskList.get(i).name());
                    if (taskList.get(i).status().state().equals(TaskStatus.TASK_STATE_RUNNING)) {
                        String[] uriArray = taskList.get(i).networkAttachments().get(0).addresses().get(0).split("/");
                        ImmutableList<PortConfig> numPorts = Exceptions.handleInterruptedCall(() -> dockerClient.inspectService(serviceList.get(0).id()).endpoint().spec().ports());
                        for (int k = 0; k < numPorts.size(); k++) {
                            int port = numPorts.get(k).publishedPort();
                            log.info("Port {}", port);
                            log.info("Uri list {}", uriArray[0]);
                            URI uri = URI.create("tcp://" + uriArray[0] + ":" + port);
                            uriList.add(uri);
                        }
                    }
                }
            }
        } catch (DockerException e) {
            log.error("Unable to list service details", e);
        }
        return uriList;
    }

    @Override
    public CompletableFuture<Void> scaleService(final int instanceCount) {
        try {
            Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);

            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            TaskSpec taskSpec = Exceptions.handleInterruptedCall(() -> dockerClient.listServices(criteria).get(0).spec().taskTemplate());
            String serviceId = Exceptions.handleInterruptedCall(() -> dockerClient.listServices(criteria).get(0).id());
            EndpointSpec endpointSpec = Exceptions.handleInterruptedCall(() -> dockerClient.inspectService(serviceId).spec().endpointSpec());
            Service service = Exceptions.handleInterruptedCall(() -> dockerClient.inspectService(serviceId));
            Exceptions.handleInterrupted(() -> dockerClient.updateService(serviceId, service.version().index(), ServiceSpec.builder().endpointSpec(endpointSpec).mode(ServiceMode.withReplicas(instanceCount)).taskTemplate(taskSpec).name(serviceName).build()));

            return Exceptions.handleInterruptedCall(() -> waitUntilServiceRunning());
        } catch (DockerException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Test failure: Unable to scale service to given instances=" + instanceCount, e);
        }
    }

    @Override
    public void stop() {
        try {
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            List<Service> serviceList = Exceptions.handleInterruptedCall(() -> dockerClient.listServices(criteria));
            for (int i = 0; i < serviceList.size(); i++) {
                String serviceId = serviceList.get(i).id();
                Exceptions.handleInterrupted(() -> dockerClient.removeService(serviceId));
            }
        } catch (DockerException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Unable to remove service.", e);
        }
    }

    public void start(final boolean wait, final ServiceSpec serviceSpec) {
        try {
            ServiceCreateResponse serviceCreateResponse = Exceptions.handleInterruptedCall(() -> dockerClient.createService(serviceSpec));
            if (wait) {
                Exceptions.handleInterrupted(() -> waitUntilServiceRunning().get(5, TimeUnit.MINUTES));
            }
            assertNotNull(serviceCreateResponse.id());
        } catch (Exception e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Unable to create service", e);
        }
    }

    // Default Health Check which uses netstat command to ensure the service is  up and running.
    List<String> defaultHealthCheck(int port) {
        return  customHealthCheck("netstat -plnt | grep " + port + " || exit 1");
    }

    //Custom Health check with the command provided by the service.
    List<String> customHealthCheck(final String cmd) {
        final List<String> commandList = new ArrayList<>(2);
        commandList.add(CMD_SHELL);
        commandList.add(cmd);
        return commandList;
    }

    long setNanoCpus(final double cpu) {
        return (long) (cpu * Math.pow(10.0, 9.0));
    }

    long setMemInBytes(final double mem) {
        return (long) mem * 1024 * 1024;
    }
}
