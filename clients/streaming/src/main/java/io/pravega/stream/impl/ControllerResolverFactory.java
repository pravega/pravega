/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.stream.impl;

import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Attributes;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.ResolvedServerInfo;
import io.grpc.ResolvedServerInfoGroup;
import io.grpc.StatusRuntimeException;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import static io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;

/**
 * gRPC Factory for resolving controller host ips and ports.
 */
@Slf4j
@ThreadSafe
public class ControllerResolverFactory extends NameResolver.Factory {

    // Use this scheme when client want to connect to a static set of controller servers.
    // Eg: tcp://ip1:port1,ip2:port2
    private final static String SCHEME_DIRECT = "tcp";

    // Use this scheme when client only knows a subset of controllers and wants other controller instances to be
    // auto discovered.
    // Eg: pravega://ip1:port1,ip2:port2
    private final static String SCHEME_DISCOVER = "pravega";

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        final String scheme = targetUri.getScheme();
        if (!SCHEME_DISCOVER.equals(scheme) && !SCHEME_DIRECT.equals(scheme)) {
            return null;
        }

        final String authority = targetUri.getAuthority();
        final List<InetSocketAddress> addresses = Splitter.on(',').splitToList(authority).stream().map(host -> {
            final String[] strings = host.split(":");
            return new InetSocketAddress(strings[0], Integer.valueOf(strings[1]));
        }).collect(Collectors.toList());

        return new ControllerNameResolver(authority, addresses, SCHEME_DISCOVER.equals(scheme));
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME_DIRECT;
    }

    @ThreadSafe
    private static class ControllerNameResolver extends NameResolver {
        // The authority part of the URI string which contains the list of server ip:port pair to connect to.
        private final String authority;

        // The initial set of servers using which we will fetch all the remaining controller instances.
        private final List<InetSocketAddress> bootstrapServers;

        // If the pravega:// scheme is used we will fetch the list of controllers from the bootstrapped servers.
        private final boolean enableDiscovery;

        // To verify the startup state of this instance.
        private final AtomicBoolean started = new AtomicBoolean(false);

        // The supplied gRPC listener using which we need to updated the controller server list.
        private Listener resolverUpdater = null;

        // Executor to schedule the controller discovery process.
        private ScheduledExecutorService scheduledExecutor = null;

        // The controller RPC client required for calling the discovery API.
        private ControllerServiceGrpc.ControllerServiceBlockingStub client = null;

        /**
         * Creates the NameResolver instance.
         *
         * @param authority         The authority string used to create the URI.
         * @param bootstrapServers  The initial set of controller endpoints.
         * @param enableDiscovery   Whether to use the controller's discovery API.
         */
        ControllerNameResolver(final String authority, final List<InetSocketAddress> bootstrapServers,
                                      final boolean enableDiscovery) {
            this.authority = authority;
            this.bootstrapServers = bootstrapServers;
            this.enableDiscovery = enableDiscovery;
        }

        @Override
        public String getServiceAuthority() {
            return this.authority;
        }

        @Override
        @Synchronized
        public void start(Listener listener) {
            Preconditions.checkState(started.compareAndSet(false, true));
            this.resolverUpdater = listener;
            if (this.enableDiscovery) {
                // We will use the direct scheme to talk to the controller bootstrap servers.
                String connectString = "tcp://";
                final List<String> strings = this.bootstrapServers.stream()
                        .map(server -> server.getHostString() + ":" + server.getPort())
                        .collect(Collectors.toList());
                connectString = connectString + String.join(",", strings);

                this.client = ControllerServiceGrpc.newBlockingStub(ManagedChannelBuilder
                        .forTarget(connectString)
                        .nameResolverFactory(new ControllerResolverFactory())
                        .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                        .usePlaintext(true)
                        .build());
                this.scheduledExecutor = Executors.newScheduledThreadPool(1,
                        new ThreadFactoryBuilder().setNameFormat("fetch-controllers-%d").setDaemon(true).build());
                this.scheduledExecutor.scheduleWithFixedDelay(
                        this::getControllers, 0L, 120L, TimeUnit.SECONDS);
            } else {
                // Use the bootstrapped server list as the final set of controllers.
                final ResolvedServerInfoGroup serverInfoGroup = ResolvedServerInfoGroup.builder().addAll(
                        this.bootstrapServers.stream().map(ResolvedServerInfo::new)
                                .collect(Collectors.toList()))
                        .build();
                log.info("Updating client with controllers: {}", serverInfoGroup);
                this.resolverUpdater.onUpdate(Collections.singletonList(serverInfoGroup), Attributes.EMPTY);
            }
        }

        @Override
        @Synchronized
        public void shutdown() {
            Preconditions.checkState(started.compareAndSet(true, false));
            if (this.scheduledExecutor != null) {
                this.scheduledExecutor.shutdownNow();
            }
        }

        // The controller discovery API invoker.
        private void getControllers() {
            try {
                final ServerResponse controllerServerList =
                        this.client.getControllerServerList(ServerRequest.getDefaultInstance());
                final ResolvedServerInfoGroup serverInfoGroup = ResolvedServerInfoGroup.builder()
                        .addAll(controllerServerList.getNodeURIList()
                                .stream()
                                .map(node ->
                                        new ResolvedServerInfo(
                                                new InetSocketAddress(node.getEndpoint(), node.getPort())))
                                .collect(Collectors.toList()))
                        .build();

                // Update gRPC load balancer with the new set of server addresses.
                log.info("Updating client with controllers: {}", serverInfoGroup);
                this.resolverUpdater.onUpdate(Collections.singletonList(serverInfoGroup), Attributes.EMPTY);
            } catch (StatusRuntimeException e) {
                log.warn("Failed to fetch controller addresses - {}", e);
                this.resolverUpdater.onError(e.getStatus());
            }
        }
    }
}
