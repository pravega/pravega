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
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import static io.pravega.test.system.framework.DockerRemoteSequential.DOCKER_CLIENT_PORT;
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

@Slf4j
public abstract class DockerBasedService implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final String IMAGE_PATH = System.getProperty("dockerImageRegistry");
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    static final String DOCKER_NETWORK = "docker-network";
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
        try {
            Service.Criteria criteria1 = Service.Criteria.builder().serviceName(serviceName).build();
            List<Service> serviceList1 = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria1));
            log.info("service list size in isRunning  {}", serviceList1.size());
            if (!serviceList1.isEmpty()) {
                long replicas = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceList1.get(0).id()).spec().mode().replicated().replicas());
                log.info("replicas {}", replicas);
                List<Container> containerList = Exceptions.handleInterrupted(() -> dockerClient.listContainers(DockerClient.ListContainersParam.withLabel("com.docker.swarm.service.name", serviceName)));
                log.info("container list size in isRunning {}", containerList.size());
                if (containerList.size() == replicas) {
                    value = true;
                }
            }
        } catch (DockerException e) {
            log.error("Unable to list docker services", e);
        }
        return value;
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
        log.debug("IS RUNNING {}", isRunning());
        return FutureHelpers.loop(() -> !isRunning(), //condition
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(5), executorService),
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
            List<Service> serviceList2 = Exceptions.handleInterrupted(() -> dockerClient.listServices(criteria2));
            log.info("service list size in getServiceDetails  {}", serviceList2.size());
            if (!serviceList2.isEmpty()) {
                ImmutableList<PortConfig> numPorts = Exceptions.handleInterrupted(() -> dockerClient.inspectService(serviceList2.get(0).id()).spec().endpointSpec().ports());
                List<Container> containerList = Exceptions.handleInterrupted(() -> dockerClient.listContainers(DockerClient.ListContainersParam.withLabel("com.docker.swarm.service.name", serviceName)));
                log.info("container size in getServiceDetails {}", containerList.size());
                for (int i = 0; i < containerList.size(); i++) {
                    String[] uriArray = containerList.get(i).networkSettings().networks().get("docker-network").ipAddress().split("/");
                    log.info("uri array {}", uriArray[0]);
                    log.info("port list size {}", numPorts.size());
                    for (int k = 0; k < numPorts.size(); k++) {
                        int port = numPorts.get(k).publishedPort();
                        log.info("port {}", port);
                        log.info("uri list {}", uriArray[0]);
                        URI uri = URI.create("tcp://" + uriArray[0] + ":" + port);
                        uriList.add(uri);
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
            log.error("Unable to scale service", e);
        }
    }

    @Override
    public void stop() {
        try {
            List<Network> networkList = Exceptions.handleInterrupted(() -> dockerClient.listNetworks(DockerClient.ListNetworksParam.byNetworkName(DOCKER_NETWORK)));
            Exceptions.handleInterrupted(() -> dockerClient.removeNetwork(networkList.get(0).id()));
            Exceptions.handleInterrupted(() -> dockerClient.leaveSwarm(true));
        } catch (DockerException e) {
            log.error("Unable to leave swarm", e);
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
