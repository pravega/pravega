/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services;

import com.google.common.base.Preconditions;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import com.spotify.docker.client.messages.Network;
import com.spotify.docker.client.messages.*;
import com.spotify.docker.client.messages.swarm.*;
import com.spotify.docker.client.messages.swarm.Service;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public abstract class DockerBasedService  implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    static final int CONTROLLER_PORT = 9092;
    final DockerClient docker;
    String serviceName;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    DockerBasedService(String serviceName) {
        this.docker = Dockerclient.getClient();
        this.serviceName = serviceName;
    }

    @Override
    public String getID() {
        Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
        String serviceId = null;
        try {
            List<Service> serviceList = docker.listServices(criteria);
            serviceId = serviceList.get(0).id();
        }  catch (DockerException | InterruptedException  e) {
            log.error("unable to get service id {}", e);
        }
        return serviceId;
    }

    @Override
    public boolean isRunning() {
       boolean value = false;
        try {
           //list the service with filter 'serviceName'
           Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
           List<Service> serviceList = docker.listServices(criteria);
           for(int i=0;i< serviceList.size();i++) {
               String serviceId = serviceList.get(i).id();
               if(!serviceId.equals(null))
                   value = true;
                   break;
           }

       } catch (DockerException | InterruptedException e) {
           log.error("unable to list docker services {}", e);
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
            List<Service> serviceList = docker.listServices(criteria);

            for (int i = 0; i < serviceList.size(); i++) {
                URI uri = URI.create("tcp://" + docker.getHost() + ":"
                        + serviceList.get(i).spec().endpointSpec()
                        .ports().get(i).targetPort());
                uriList.add(uri);
            }
        }  catch (InterruptedException | DockerException e) {
            log.error("unable to list service details {}", e);
        }
        return uriList;
    }

    @Override
    public void scaleService(final int instanceCount, final boolean wait) {
        try {
            Preconditions.checkArgument(instanceCount >= 0, "negative value: %s", instanceCount);
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            String serviceId = docker.listServices(criteria).get(0).id();
            docker.updateService(serviceId, 1L, ServiceSpec.builder().mode(ServiceMode.withReplicas(instanceCount)).build());
            String updateState = docker.inspectService(serviceId).updateStatus().state();
            log.info(" update state {}", updateState);
        } catch (DockerException | InterruptedException e) {
            log.error("unable to scale service {}", e);
        }
    }

    @Override
    public void stop() {
        try {
            List<Network> networkList =  docker.listNetworks(DockerClient.ListNetworksParam.byNetworkName("network-name"));
            for(int i = 0; i < networkList.size(); i++){
             docker.removeNetwork(networkList.get(i).id());
            }
            docker.leaveSwarm(true);
        } catch (DockerException | InterruptedException e) {
            log.error("unable to leave swarm");
        }
        docker.close();
    }
}
