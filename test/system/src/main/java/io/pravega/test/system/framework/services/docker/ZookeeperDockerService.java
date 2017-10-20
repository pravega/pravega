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

import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ServiceCreateResponse;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.NetworkAttachmentConfig;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.ResourceRequirements;
import com.spotify.docker.client.messages.swarm.Resources;
import com.spotify.docker.client.messages.swarm.Service;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;


@Slf4j
public class ZookeeperDockerService extends DockerBasedService {

    private static final String ZK_IMAGE = "jplock/zookeeper:3.5.1-alpha";
    private final long instances = 1;
    private final double cpu = 1.0 * Math.pow(10.0, 9.0);
    private final long mem = 1024 * 1024 * 1024L;

    public ZookeeperDockerService(String serviceName) {
        super(serviceName);
    }

    @Override
    public void stop() {
        try {
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
            List<Service> serviceList = dockerClient.listServices(criteria);
            for (int i = 0; i < serviceList.size(); i++) {
                String serviceId = serviceList.get(i).id();
                dockerClient.removeService(serviceId);
            }
        } catch (DockerException | InterruptedException e) {
            log.error("Unable to remove service", e);
        }
    }

    @Override
    public void clean() {
    }

    @Override
    public void start(final boolean wait) {
        try {
            ServiceCreateResponse serviceCreateResponse = dockerClient.createService(setServiceSpec());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        } catch (InterruptedException | DockerException | ExecutionException | TimeoutException e) {
            log.error("Unable to create service", e);
        }
    }

    private ServiceSpec setServiceSpec() {

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image(ZK_IMAGE)
                        .hostname(serviceName)
                        .healthcheck(ContainerConfig.Healthcheck.create(null,
                                1000000000L, 1000000000L, 3)).build())
                .networks(NetworkAttachmentConfig.builder().target("docker-network").aliases(serviceName).build())
                .resources(ResourceRequirements.builder()
                        .limits(Resources.builder().memoryBytes(mem).nanoCpus((long) cpu).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target("docker-network").aliases(serviceName).build())
                .endpointSpec(EndpointSpec.builder().ports(PortConfig.builder().publishedPort(ZKSERVICE_ZKPORT).targetPort(ZKSERVICE_ZKPORT).publishMode(PortConfig.PortConfigPublishMode.HOST).build()).build())
                .taskTemplate(taskSpec)
                .mode(ServiceMode.withReplicas(instances)).build();
        return spec;
    }
}
