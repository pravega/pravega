/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.system.framework.services.docker;

import com.google.common.base.Strings;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ServiceCreateResponse;
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
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class PravegaSegmentStoreDockerService extends DockerBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private static final String SEGMENTSTORE_EXTRA_ENV = System.getProperty("segmentStoreExtraEnv");
    private static final String ENV_SEPARATOR = ";;";
    private static final java.lang.String KEY_VALUE_SEPARATOR = "::";
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 1000.0;

    public PravegaSegmentStoreDockerService(final String serviceName) {
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
        } catch (InterruptedException | DockerException | TimeoutException | ExecutionException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("volume").source("logs-volume").target("/tmp/logs").build();
        //set env
        String zk = "zookeeper:" + ZKSERVICE_ZKPORT;
        String con = "controller" + CONTROLLER_PORT;
        //System properties to configure SS service.
        String hostSystemProperties =
                setSystemProperty("autoScale.muteInSeconds", "120") +
                setSystemProperty("autoScale.cooldownInSeconds", "120") +
                setSystemProperty("autoScale.cacheExpiryInSeconds", "120") +
                setSystemProperty("autoScale.cacheCleanUpInSeconds", "120") +
                setSystemProperty("log.level", "DEBUG") +
                setSystemProperty("curator-default-session-timeout", String.valueOf(30 * 1000))+
                        setSystemProperty("hdfs.replaceDataNodesOnFailure", "false");

        String env1 = "PRAVEGA_SEGMENTSTORE_OPTS=" + hostSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx900m";
        String env3 = "ZK_URL=" + zk;
        String env4 = "BK_ZK_URL=" + zk;
        String env5 = "CONTROLLER_URL=" + con;
        List<String> stringList = new ArrayList<>();
        stringList.add(env1);
        stringList.add(env2);
        getCustomEnvVars(stringList, SEGMENTSTORE_EXTRA_ENV);
        stringList.add(env3);
        stringList.add(env4);
        stringList.add(env5);

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L, 3))
                        .mounts(Arrays.asList(mount))
                        .env(stringList).args("segmentstore").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target("docker-network").build())
                .endpointSpec(EndpointSpec.builder().addPort(PortConfig.builder().
                        targetPort(SEGMENTSTORE_PORT).protocol("TCP").build())
                        .build()).build();
        return spec;
    }

    private void getCustomEnvVars(List<String> stringList, String segmentstoreExtraEnv) {
        log.info("Extra segment store env variables are {}", segmentstoreExtraEnv);
        if (!Strings.isNullOrEmpty(segmentstoreExtraEnv)) {
            Arrays.stream(segmentstoreExtraEnv.split(ENV_SEPARATOR)).forEach(str -> {
                String[] pair = str.split(KEY_VALUE_SEPARATOR);
                if (pair.length != 2) {
                    log.warn("Key Value not present {}", str);
                } else {
                    stringList.add(pair[0].toString() + "=" + pair[1].toString());
                }
            });
        } else {
            // Set HDFS as the default for Tier2.
            stringList.add("HDFS_URL=" + "hdfs:8020");
            stringList.add("TIER2_STORAGE=HDFS");
        }
    }
}
