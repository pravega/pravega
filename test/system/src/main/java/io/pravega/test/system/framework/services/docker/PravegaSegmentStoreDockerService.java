/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.test.system.framework.services.docker;

import com.google.common.base.Strings;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;

@Slf4j
public class PravegaSegmentStoreDockerService extends DockerBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private static final String SEGMENTSTORE_EXTRA_ENV = System.getProperty("segmentStoreExtraEnv");
    private static final String ENV_SEPARATOR = ";;";
    private static final String KEY_VALUE_SEPARATOR = "::";
    private final long instances = 1;
    private final double cpu = 0.5;
    private final double mem = 1741.0;
    private final URI zkUri;
    private final URI conUri;
    private final URI hdfsUri;

    public PravegaSegmentStoreDockerService(final String serviceName, final URI zkUri, final URI hdfsUri, final URI conUri) {
        super(serviceName);
        this.zkUri = zkUri;
        this.hdfsUri = hdfsUri;
        this.conUri = conUri;
    }

    @Override
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
        Map<String, String> labels = new HashMap<>();
        labels.put("com.docker.swarm.task.name", serviceName);

        Mount mount = Mount.builder().type("volume").source("segmentstore-logs").target("/opt/pravega/logs").build();
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        //System properties to configure SS service.
        Map<String, String> stringBuilderMap = new HashMap<>();
        StringBuilder systemPropertyBuilder = new StringBuilder();
        stringBuilderMap.put("autoScale.muteInSeconds", "120");
        stringBuilderMap.put("autoScale.cooldownInSeconds", "120");
        stringBuilderMap.put("autoScale.cacheExpiryInSeconds", "120");
        stringBuilderMap.put("autoScale.cacheCleanUpInSeconds", "120");
        stringBuilderMap.put("log.level", "DEBUG");
        stringBuilderMap.put("curator-default-session-timeout", String.valueOf(30 * 1000));
        stringBuilderMap.put("hdfs.replaceDataNodesOnFailure", "false");
        stringBuilderMap.put("bookkeeper.bkAckQuorumSize", "3");
        for (Map.Entry<String, String> entry : stringBuilderMap.entrySet()) {
            systemPropertyBuilder.append("-D").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }

        String hostSystemProperties = systemPropertyBuilder.toString();

        //set env
        String env1 = "PRAVEGA_SEGMENTSTORE_OPTS=" + hostSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx2000m";
        List<String> envList = new ArrayList<>();
        envList.add(env1);
        envList.add(env2);
        getCustomEnvVars(envList, SEGMENTSTORE_EXTRA_ENV, hdfsUri);
        String env3 = "ZK_URL=" + zk;
        String env4 = "BK_ZK_URL=" + zk;
        String env5 = "CONTROLLER_URL=" + conUri.toString();
        envList.add(env3);
        envList.add(env4);
        envList.add(env5);

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).build())
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + IMAGE_PREFIX + PRAVEGA_IMAGE_NAME + PRAVEGA_VERSION)
                        .hostname(serviceName)
                        .labels(labels)
                        .healthcheck(ContainerConfig.Healthcheck.builder().test(defaultHealthCheck(SEGMENTSTORE_PORT)).build())
                        .mounts(Arrays.asList(mount))
                        .env(envList).args("segmentstore").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder().ports(PortConfig.builder().
                        publishedPort(SEGMENTSTORE_PORT).targetPort(SEGMENTSTORE_PORT).publishMode(PortConfig.PortConfigPublishMode.HOST).build())
                        .build()).build();
        return spec;
    }

    private void getCustomEnvVars(List<String> stringList, String segmentstoreExtraEnv, final URI hdfsUri) {
        log.info("Extra segment store env variables are {}", segmentstoreExtraEnv);
        if (!Strings.isNullOrEmpty(segmentstoreExtraEnv)) {
            Arrays.stream(segmentstoreExtraEnv.split(ENV_SEPARATOR)).forEach(str -> {
                String[] pair = str.split(KEY_VALUE_SEPARATOR);
                if (pair.length != 2) {
                    log.warn("Key Value not present {}", str);
                    throw new RuntimeException("No key value pair to set Tier2 storage env");
                } else {
                    stringList.add(pair[0].toString() + "=" + pair[1].toString());
                }
            });
        } else {
            // Set HDFS as the default for Tier2.
            stringList.add("HDFS_URL=" + hdfsUri.getHost() + ":8020");
            stringList.add("TIER2_STORAGE=HDFS");
        }
    }
}
