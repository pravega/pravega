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
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import com.spotify.docker.client.messages.swarm.TaskStatus;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import static io.pravega.test.system.framework.DockerRemoteSequential.DOCKER_CLIENT_PORT;
import io.pravega.test.system.framework.Utils;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import com.spotify.docker.client.messages.Network;
import com.spotify.docker.client.messages.swarm.Task;

@Slf4j
public abstract class DockerBasedService implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final String IMAGE_PATH = System.getProperty("dockerImageRegistry");
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    final DockerClient dockerClient;
    final String serviceName;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    DockerBasedService(final String serviceName) {
        this.dockerClient = DefaultDockerClient.builder().uri("http://" + System.getProperty("masterIP") + ":" + DOCKER_CLIENT_PORT).build();
        this.serviceName = serviceName;
    }

    @Override
    public String getID() {
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        String serviceId = null;
        try {
            List<Service> serviceList = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria));
            serviceId = serviceList.get(0).id();
        } catch (DockerException e) {
            log.error("Unable to get service id", e);
        }
        return serviceId;
    }

    @Override
    public boolean isRunning() {
        boolean value = false;
        int taskRunningCount = 0;
        try {
            //list the service with filter 'serviceName'
            Service.Criteria criteria1 = Service.Criteria.builder().serviceName(serviceName).build();
            List<Service> serviceList1 = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria1));
            log.info("service list size {}", serviceList1.size());
            Task.Criteria taskCriteria1 = Task.Criteria.builder().serviceName(serviceName).build();
            List<Task> taskList1 = Exceptions.handleInterrupted(() ->  dockerClient.listTasks(taskCriteria1));
            log.info("task list size {}", taskList1.size());

            if (!taskList1.isEmpty()) {
                for (int j = 0; j < taskList1.size(); j++) {
                    log.info("task {}", taskList1.get(j).name());
                    String state = taskList1.get(j).status().state();
                    log.info("task state {}", state);
                    if (state.equals(TaskStatus.TASK_STATE_RUNNING)) {
                        taskRunningCount++;
                    }
                }
            }

            if (!serviceList1.isEmpty()) {
                long replicas = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceList1.get(0).id()).spec().mode().replicated().replicas());
                log.info("replicas {}", replicas);
                log.info("task running count {}", taskRunningCount);
                if (((long) taskRunningCount) == replicas) {
                    return true;
                }
            }
        } catch (DockerException e) {
            throw new AssertionError("Unable to list docker services", e);
        }
        return value;
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
        log.debug("IS RUNNING {}", isRunning());
        return Futures.loop(() -> !isRunning(), //condition
                () -> Futures.delayedFuture(Duration.ofSeconds(5), executorService),
                executorService);
    }

    String setSystemProperty(final String propertyName, final String propertyValue) {
        return new StringBuilder().append(" -D").append(propertyName).append("=").append(propertyValue).toString();
    }

    @Override
    public List<URI> getServiceDetails() {
        Service.Criteria criteria2 = Service.Criteria.builder().serviceName(this.serviceName).build();
        List<URI> uriList = new ArrayList<>();
        try {
            Task.Criteria taskCriteria2 = Task.Criteria.builder().taskName(serviceName).build();
            List<Task> taskList2 = Exceptions.handleInterrupted(() -> dockerClient.listTasks(taskCriteria2));
            log.info("task size {}", taskList2.size());

            if (!taskList2.isEmpty()) {
                log.info("network addresses {}", taskList2.get(0).networkAttachments().get(0).addresses().get(0));
                List<Service> serviceList2 = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria2));
                log.info("service list size {}", serviceList2.size());
                for (int i = 0; i < taskList2.size(); i++) {
                    log.info("task {}", taskList2.get(i).name());
                    if (taskList2.get(i).status().state().equals(TaskStatus.TASK_STATE_RUNNING)) {
                    String[] uriArray = taskList2.get(i).networkAttachments().get(0).addresses().get(0).split("/");
                    ImmutableList<PortConfig> numPorts = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceList2.get(0).id()).endpoint().spec().ports());
                    for (int k = 0; k < numPorts.size(); k++) {
                        int port = numPorts.get(k).publishedPort();
                        log.info("port {}", port);
                        log.info("uri list {}", uriArray[0]);
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
    public void scaleService(final int instanceCount, final boolean wait) {
        try {
            Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            TaskSpec taskSpec = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria).get(0).spec().taskTemplate());
            String serviceId = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria).get(0).id());
            EndpointSpec endpointSpec = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceId).spec().endpointSpec());
            Service service = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceId));
            Exceptions.handleInterrupted(() -> dockerClient.updateService(serviceId, service.version().index(), ServiceSpec.builder().endpointSpec(endpointSpec).mode(ServiceMode.withReplicas(instanceCount)).taskTemplate(taskSpec).name(serviceName).build()));
            String updateState = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceId).updateStatus().state());
            log.info("Update state {}", updateState);
            if (wait) {
                Exceptions.handleInterrupted(() -> waitUntilServiceRunning().get());
            }
        } catch (ExecutionException | DockerException e) {
            throw new AssertionError("Unable to scale service to given instances.Test Failure", e);
        }
    }

    @Override
    public void stop() {
        try {
            List<Network> networkList = Exceptions.handleInterrupted(() -> dockerClient.listNetworks(DockerClient.ListNetworksParam.byNetworkName(Utils.DOCKER_NETWORK)));
            Exceptions.handleInterrupted(() -> dockerClient.removeNetwork(networkList.get(0).id()));
            Exceptions.handleInterrupted(() -> dockerClient.leaveSwarm(true));
        } catch (DockerException e) {
           throw new AssertionError("Unable to leave Swarm. Test Failure", e);
        }
        dockerClient.close();
    }

    long setNanoCpus(final double cpu) {
        return (long) (cpu * Math.pow(10.0, 9.0));
    }

    long setMemInBytes(final double mem) {
        return (long) mem * 1024 * 1024;
    }
}
