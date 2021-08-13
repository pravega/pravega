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

import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.NetworkAttachmentConfig;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.ResourceRequirements;
import com.spotify.docker.client.messages.swarm.Resources;
import com.spotify.docker.client.messages.swarm.RestartPolicy;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import static com.spotify.docker.client.messages.swarm.RestartPolicy.RESTART_POLICY_ANY;
import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;

@Slf4j
public class BookkeeperDockerService extends DockerBasedService {

    private static final int BK_PORT = 3181;
    private static final String BOOKKEEPER_IMAGE_NAME = System.getProperty("bookkeeperImageName", "bookkeeper") + ":";
    private final long instances = 3;
    private final double cpu = 0.5;
    private final double mem = 1024.0;
    private final URI zkUri;

    public BookkeeperDockerService(String serviceName, final URI zkUri) {
        super(serviceName);
        this.zkUri = zkUri;
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
        labels.put("com.docker.swarm.service.name", serviceName);

        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        List<String> stringList = new ArrayList<>();
        String env1 = "ZK_URL=" + zk;
        String env2 = "ZK=" + zk;
        String env3 = "bookiePort=" + String.valueOf(BK_PORT);
        String env4 = "DLOG_EXTRA_OPTS=-Xms512m";
        String env5 = "BK_useHostNameAsBookieID=false";
        stringList.add(env1);
        stringList.add(env2);
        stringList.add(env3);
        stringList.add(env4);
        stringList.add(env5);

        final TaskSpec taskSpec = TaskSpec
                .builder().restartPolicy(RestartPolicy.builder().maxAttempts(3).condition(RESTART_POLICY_ANY).build())
                .containerSpec(ContainerSpec.builder()
                        .hostname(serviceName)
                        .labels(labels)
                        .image(IMAGE_PATH + IMAGE_PREFIX + BOOKKEEPER_IMAGE_NAME + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.builder().test(defaultHealthCheck(BK_PORT)).build())
                        .env(stringList).build())
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .endpointSpec(EndpointSpec.builder().ports(PortConfig.builder().publishedPort(BK_PORT).build()).build())
                .taskTemplate(taskSpec)
                .build();
        return spec;
    }
}
