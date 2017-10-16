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
import com.spotify.docker.client.messages.mount.Mount;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.NetworkAttachmentConfig;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.ResourceRequirements;
import com.spotify.docker.client.messages.swarm.Resources;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class PravegaControllerDockerService extends DockerBasedService {

    static final int CONTROLLER_PORT = 9090;
    private static final int REST_PORT = 9091;
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 700.0;

    public PravegaControllerDockerService(final String serviceName) {
        super(serviceName);
    }

    @Override
    public void stop() {
        try {
            dockerClient.removeService(getID());
        } catch (DockerException | InterruptedException e) {
            log.error("unable to remove service {}", e);
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
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("Volume").source("volume-logs").target("/tmp/logs").build();
        String zk = "zookeeper:" + ZKSERVICE_ZKPORT;
        String controllerSystemProperties = setSystemProperty("ZK_URL", zk) +
                setSystemProperty("CONTROLLER_RPC_PUBLISHED_HOST", "controller") +
                setSystemProperty("CONTROLLER_RPC_PUBLISHED_PORT", String.valueOf(CONTROLLER_PORT)) +
                setSystemProperty("CONTROLLER_SERVER_PORT", String.valueOf(CONTROLLER_PORT)) +
                setSystemProperty("REST_SERVER_PORT", String.valueOf(REST_PORT)) +
                setSystemProperty("log.level", "DEBUG") +
                setSystemProperty("curator-default-session-timeout", String.valueOf(10 * 1000));
        String env1 = "PRAVEGA_CONTROLLER_OPTS=" + controllerSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx512m";
        List<String> stringList = new ArrayList<>();
        stringList.add(env1);
        stringList.add(env2);
        final TaskSpec taskSpec = TaskSpec
                .builder()
                .networks(NetworkAttachmentConfig.builder().target("docker-network").build())
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L, 3))
                        .mounts(Arrays.asList(mount))
                        .env(stringList).args("controller").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        List<PortConfig> portConfigs = new ArrayList<>();
        PortConfig port1 = PortConfig.builder().publishedPort(CONTROLLER_PORT).build();
        PortConfig port2 = PortConfig.builder().publishedPort(REST_PORT).build();
        portConfigs.add(port1);
        portConfigs.add(port2);
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder().ports(portConfigs)
                        .build()).build();
        return spec;
    }
}
