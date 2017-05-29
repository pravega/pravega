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
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.Swarm;
import com.spotify.docker.client.messages.swarm.SwarmInit;
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
    final DockerClient docker;
    String serviceName;
    Swarm swarm;


    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    DockerBasedService(String serviceName) {

        //create a docker client
        this.docker = DefaultDockerClient.builder().uri("http://localhost:2375").build();
        try {
        //create a single-node swarm on the current node.
        swarm = docker.inspectSwarm();
        //log.info("swarm id = {}"  swarm.id());

        if (swarm.id() == null) {
            final String nodeId;

                nodeId = docker.initSwarm(SwarmInit.builder()
                        .advertiseAddr("localhost")
                        .listenAddr("0.0.0.0:2377")
                        .build()
                );
                assertThat(nodeId, is(notNullValue()));
            }
        }  catch (DockerException | InterruptedException e) {
                log.error( "unable to initialize a new swarm {}", e);
        }
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
        boolean value = true;
       try {
           //list the service with filter 'serviceName'
           Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
           List<Service> serviceList = docker.listServices(criteria);
           if (serviceList.isEmpty()) {
               value = false;
           }
       } catch (DockerException | InterruptedException e) {
           log.error("unable to list docker services {}", e);
       }
       return value;
    }

    CompletableFuture<Void> waitUntilServiceRunning() {
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

            for (int i = 0; i < uriList.size(); i++) {
                URI uri = URI.create("TCP://" + docker.getHost() + ":" + serviceList.get(i).spec().endpointSpec().ports().get(i).publishedPort());
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
}
