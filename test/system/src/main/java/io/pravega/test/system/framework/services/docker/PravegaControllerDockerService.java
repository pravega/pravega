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

import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.mount.Mount;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.NetworkAttachmentConfig;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.ResourceRequirements;
import com.spotify.docker.client.messages.swarm.Resources;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static io.pravega.test.system.framework.Utils.DOCKER_CONTROLLER_PORT;
import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;
import static io.pravega.test.system.framework.Utils.REST_PORT;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaControllerDockerService extends DockerBasedService {

    private final int instances = 1;
    private final double cpu = 0.5;
    private final double mem = 700.0;
    private final URI zkUri;

    public PravegaControllerDockerService(final String serviceName, final URI zkUri) {
        super(serviceName);
        this.zkUri = zkUri;
    }

    public void stop() {
        super.stop();
    }

    @Override
    public void clean() {
    }

    @Override
    public void start(final boolean wait) {
        start(wait, setServiceSpec());
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("Volume").source("volume-logs").target("/tmp/logs").build();
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        Map<String, String> stringBuilderMap = new HashMap<>();
        stringBuilderMap.put("ZK_URL", zk);
        stringBuilderMap.put("CONTROLLER_RPC_PUBLISHED_HOST", serviceName);
        stringBuilderMap.put("CONTROLLER_RPC_PUBLISHED_PORT", String.valueOf(DOCKER_CONTROLLER_PORT));
        stringBuilderMap.put("CONTROLLER_SERVER_PORT", String.valueOf(DOCKER_CONTROLLER_PORT));
        stringBuilderMap.put("REST_SERVER_PORT", String.valueOf(REST_PORT));
        stringBuilderMap.put("log.level", "DEBUG");
        stringBuilderMap.put("curator-default-session-timeout", String.valueOf(10 * 1000));
        stringBuilderMap.put("ZK_SESSION_TIMEOUT_MS", String.valueOf(30 * 1000));
        stringBuilderMap.put("MAX_LEASE_VALUE", String.valueOf(60 * 1000));
        stringBuilderMap.put("MAX_SCALE_GRACE_PERIOD", String.valueOf(60 * 1000));
        stringBuilderMap.put("RETENTION_FREQUENCY_MINUTES", String.valueOf(2));
        StringBuilder systemPropertyBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : stringBuilderMap.entrySet()) {
            systemPropertyBuilder.append("-D").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }

        String controllerSystemProperties = systemPropertyBuilder.toString();
        String env1 = "PRAVEGA_CONTROLLER_OPTS=" + controllerSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx512m";
        Map<String, String> labels = new HashMap<>();
        labels.put("com.docker.swarm.task.name", serviceName);
        final TaskSpec taskSpec = TaskSpec
                .builder()
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + "nautilus/pravega:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, Duration.ofSeconds(10).toNanos(), Duration.ofSeconds(10).toNanos(), 3))
                        .mounts(Arrays.asList(mount))
                        .hostname(serviceName)
                        .labels(labels)
                        .env(Arrays.asList(env1, env2)).args("controller").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder()
                        .ports(Arrays.asList(PortConfig.builder()
                                        .publishedPort(DOCKER_CONTROLLER_PORT).targetPort(DOCKER_CONTROLLER_PORT).publishMode(PortConfig.PortConfigPublishMode.HOST).build(),
                                PortConfig.builder().publishedPort(REST_PORT).targetPort(REST_PORT).publishMode(PortConfig.PortConfigPublishMode.HOST).build())).
                                build())
                .build();
        return spec;
    }
}
