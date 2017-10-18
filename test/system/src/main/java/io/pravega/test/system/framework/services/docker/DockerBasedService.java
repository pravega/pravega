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
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public abstract class DockerBasedService  implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final int CONTROLLER_PORT = 9090;
    static final String IMAGE_PATH = System.getProperty("dockerImageRegistry");
    static final String PRAVEGA_VERSION = System.getProperty("imageVersion");
    DockerClient dockerClient;
    String serviceName;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    DockerBasedService(String serviceName) {
        dockerClient = DefaultDockerClient.builder().uri("http://"+ System.getProperty("masterIP")+ ":2375").build();
        this.serviceName = serviceName;
        }


    @Override
    public String getID() {
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        String serviceId = null;
        try {
            List<Service> serviceList = dockerClient.listServices(criteria);
            serviceId = serviceList.get(0).id();
        }  catch (DockerException | InterruptedException  e) {
            log.error("Unable to get service id {}", e);
        }
        return serviceId;
    }

    @Override
    public boolean isRunning() {
        boolean value = false;
        try {
            //list the service with filter 'serviceName'
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            List<Service> serviceList = dockerClient.listServices(criteria);
            int containerCount = dockerClient.listContainers(DockerClient.ListContainersParam.filter("name", serviceName)).size();
            if (!serviceList.isEmpty()) {
                long replicas = dockerClient.inspectService(serviceList.get(0).id()).spec().mode().replicated().replicas();
                if (((long) containerCount) == replicas) {
                    return true;
                }
            }
        } catch (DockerException | InterruptedException e) {
            log.error("Unable to list docker services {}", e);
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
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        List<URI> uriList = new ArrayList<>();
        try {
            List<Container> containerList = dockerClient.listContainers(DockerClient.ListContainersParam.withLabel("com.docker.swarm.service.name", serviceName));
            log.info("container list size {}", containerList.size());
            Service.Criteria criteria1 = Service.Criteria.builder().serviceName(this.serviceName).build();
            List<Service> serviceList = dockerClient.listServices(criteria1);
            for (int i = 0; i < containerList.size(); i++) {
                String ip = containerList.get(i).networkSettings().networks().get("docker-network").ipAddress();
                URI uri = URI.create("tcp://" + ip + ":" + dockerClient.inspectService(serviceList.get(0).id()).endpoint().ports().get(0).publishedPort());
                uriList.add(uri);
            }
        }  catch (InterruptedException | DockerException e) {
            log.error("Unable to list service details {}", e);
        }
        return uriList;
    }

    @Override
    public void scaleService(final int instanceCount, final boolean wait) {
        try {
            Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            TaskSpec taskSpec = dockerClient.listServices(criteria).get(0).spec().taskTemplate();
            String serviceId = dockerClient.listServices(criteria).get(0).id();
            dockerClient.updateService(serviceId, 1L, ServiceSpec.builder().mode(ServiceMode.withReplicas(instanceCount)).
                    taskTemplate(taskSpec).name(serviceName).build());
            String updateState = dockerClient.inspectService(serviceId).updateStatus().state();
            log.info("update state {}", updateState);
            if (wait) {
                waitUntilServiceRunning().get();
            }
        } catch (ExecutionException | DockerException | InterruptedException e) {
            log.error("unable to scale service {}", e);
        }
    }

    @Override
    public void stop() {
        try {
            List<com.spotify.docker.client.messages.Network> networkList =  dockerClient.listNetworks(DockerClient.ListNetworksParam.byNetworkName("network-name"));
            for (int i = 0; i < networkList.size(); i++) {
                dockerClient.removeNetwork(networkList.get(i).id());
            }
            dockerClient.leaveSwarm(true);
        } catch (DockerException | InterruptedException e) {
            log.error("unable to leave swarm");
        }
        dockerClient.close();
    }

    long setNanoCpus(final double cpu) {
        return (long)  (cpu * Math.pow(10.0, 9.0));
    }

    long setMemInBytes(final double mem) {
        return (long) mem * 1024 * 1024;
    }
}
