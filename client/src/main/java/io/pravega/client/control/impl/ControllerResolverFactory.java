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
package io.pravega.client.control.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC Factory for resolving controller host ips and ports.
 */
@Slf4j
@ThreadSafe
@RequiredArgsConstructor
class ControllerResolverFactory extends NameResolver.Factory {

    // Use this scheme when client want to connect to a static set of controller servers.
    // Eg: tcp://ip1:port1,ip2:port2
    private final static String SCHEME_DIRECT = "tcp";

    // Use this scheme when client only knows a subset of controllers and wants other controller instances to be
    // auto discovered.
    // Eg: pravega://ip1:port1,ip2:port2
    private final static String SCHEME_DISCOVER = "pravega";
    //Secure version of discover scheme.
    private final static String SCHEME_DISCOVER_TLS = "pravegas";
    @NonNull
    private final ScheduledExecutorService executor;

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args params) {
        final String scheme = targetUri.getScheme();

        final String authority = targetUri.getAuthority();
        final List<InetSocketAddress> addresses = Splitter.on(',').splitToList(authority).stream().map(host -> {
            final String[] strings = host.split(":");
            Preconditions.checkArgument(strings.length == 2, "URI should have both address and port");
            return InetSocketAddress.createUnresolved(strings[0], Integer.parseInt(strings[1]));
        }).collect(Collectors.toList());

        return new ControllerNameResolver(authority, addresses, SCHEME_DISCOVER.equals(scheme) || SCHEME_DISCOVER_TLS.equals(scheme), executor);
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME_DIRECT;
    }

    @ThreadSafe
    private static class ControllerNameResolver extends NameResolver {

        // Regular controller discovery interval.
        private final static long REFRESH_INTERVAL_MS = 120000L;

        // Controller discovery retry timeout when failures are detected.
        private final static long FAILURE_RETRY_TIMEOUT_MS = 10000L;

        // The authority part of the URI string which contains the list of server ip:port pair to connect to.
        private final String authority;

        // The initial set of servers using which we will fetch all the remaining controller instances.
        private final List<InetSocketAddress> bootstrapServers;

        // If the pravega:// scheme is used we will fetch the list of controllers from the bootstrapped servers.
        private final boolean enableDiscovery;

        // The controller RPC client required for calling the discovery API.
        private final ControllerServiceGrpc.ControllerServiceBlockingStub client;

        // Executor to schedule the controller discovery process.
        private final ScheduledExecutorService scheduledExecutor;

        // The supplied gRPC listener using which we need to update the controller server list.
        private volatile Listener resolverUpdater = null;

        // The scheduledFuture for the discovery task to track future schedules.
        @GuardedBy("$lock")
        private ScheduledFuture<?> scheduledFuture = null;

        // The last update time, useful to decide when to trigger the next retry on failures.
        @GuardedBy("$lock")
        private long lastUpdateTimeMS = 0;

        // To verify the startup state of this instance.
        @GuardedBy("$lock")
        private boolean shutdown = false;

        /**
         * Creates the NameResolver instance.
         *
         * @param authority         The authority string used to create the URI.
         * @param bootstrapServers  The initial set of controller endpoints.
         * @param enableDiscovery   Whether to use the controller's discovery API.
         * @param executor          The executor to run resolve tasks on.
         */
        @SuppressWarnings("deprecation")
        ControllerNameResolver(final String authority, final List<InetSocketAddress> bootstrapServers,
                               final boolean enableDiscovery, ScheduledExecutorService executor) {
            this.authority = authority;
            this.bootstrapServers = ImmutableList.copyOf(bootstrapServers);
            this.enableDiscovery = enableDiscovery;
            if (this.enableDiscovery) {
                // We will use the direct scheme to send the discovery RPC request to the controller bootstrap servers.
                String connectString = "tcp://";
                final List<String> strings = this.bootstrapServers.stream()
                        .map(server -> server.getHostString() + ":" + server.getPort())
                        .collect(Collectors.toList());
                connectString = connectString + String.join(",", strings);

                this.client = ControllerServiceGrpc.newBlockingStub(ManagedChannelBuilder
                        .forTarget(connectString)
                        .nameResolverFactory(new ControllerResolverFactory(executor))
                        .defaultLoadBalancingPolicy("round_robin")
                        .usePlaintext()
                        .build());
            } else {
                this.client = null;
            }

            this.scheduledExecutor = executor;
        }

        @Override
        public String getServiceAuthority() {
            return this.authority;
        }

        @Override
        @Synchronized
        public void start(Listener listener) {
            Preconditions.checkState(this.resolverUpdater == null, "ControllerNameResolver has already been started");
            Preconditions.checkState(!shutdown, "ControllerNameResolver is shutdown, restart is not supported");
            this.resolverUpdater = listener;
            boolean scheduleDiscovery;
            // If the servers comprise only of IP addresses then we need to update the controller list only once.
            List<EquivalentAddressGroup> servers = new ArrayList<>();
            if (!this.enableDiscovery) {
                scheduleDiscovery = false;
                // Use the bootstrapped server list as the final set of controllers.
                for (InetSocketAddress address : bootstrapServers) {
                    if (InetAddresses.isInetAddress(address.getHostString())) {
                        servers.add(new EquivalentAddressGroup(
                                new InetSocketAddress(address.getHostString(), address.getPort())));
                    } else {
                        scheduleDiscovery = true;
                    }
                }
            } else {
                scheduleDiscovery = true;
            }
            if (scheduleDiscovery) {
                // Schedule the first discovery immediately.
                this.scheduledFuture = this.scheduledExecutor.schedule(this::getControllers, 0L, TimeUnit.SECONDS);
            } else {
                log.info("Updating client with controllers: {}", servers);
                this.resolverUpdater.onAddresses(servers, Attributes.EMPTY);  
            }
        }

        @Override
        @Synchronized
        public void shutdown() {
            shutdown = true;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        }

        @Override
        @Synchronized
        public void refresh() {
            // Refresh is called as hints when gRPC detects network failures.
            // We don't want to repeatedly attempt discovery; following logic will limit discovery on failures to
            // once every FAILURE_RETRY_TIMEOUT_MS seconds. Also we want to trigger discovery sooner on failures.
            if (!shutdown && this.resolverUpdater != null) {
                if (this.scheduledFuture != null && !this.scheduledFuture.isDone()) {
                    final long nextUpdateDuration = this.scheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                    final long lastUpdateDuration = System.currentTimeMillis() - this.lastUpdateTimeMS;
                    if (nextUpdateDuration > 0
                            && (nextUpdateDuration + lastUpdateDuration) > FAILURE_RETRY_TIMEOUT_MS) {
                        // Cancel the existing schedule and advance the discovery process.
                        this.scheduledFuture.cancel(true);

                        // Ensure there is a delay of at least FAILURE_RETRY_TIMEOUT_MS between 2 discovery attempts.
                        long scheduleDelay = 0;
                        if (lastUpdateDuration < FAILURE_RETRY_TIMEOUT_MS) {
                            scheduleDelay = FAILURE_RETRY_TIMEOUT_MS - lastUpdateDuration;
                        }
                        this.scheduledFuture = this.scheduledExecutor.schedule(
                                this::getControllers, scheduleDelay, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }

        /**
         * The controller discovery API invoker.
         * This refreshes the list of controller addresses to be used by the gRPC transport.
         * The discovery process will be rescheduled in the end after a delay which is calculated based on whether
         * the controller addresses have been fetched successfully or not.
         */
        private void getControllers() {
            log.info("Attempting to refresh the controller server endpoints");
            final List<EquivalentAddressGroup> servers;
            long nextScheduleTimeMS = REFRESH_INTERVAL_MS;
            try {
                if (this.enableDiscovery) {
                    // Make an RPC call to the bootstrapped controller servers to fetch all active controllers.
                    final ServerResponse controllerServerList =
                            this.client.getControllerServerList(ServerRequest.getDefaultInstance());
                    servers = controllerServerList.getNodeURIList().stream()
                            .map(node ->
                                    new EquivalentAddressGroup(new InetSocketAddress(node.getEndpoint(), node.getPort())))
                            .collect(Collectors.toList());
                } else {
                    // Resolve the bootstrapped server hostnames to get the set of controllers.
                    servers = new ArrayList<>();
                    this.bootstrapServers.forEach(address -> {
                        final InetSocketAddress socketAddress = new InetSocketAddress(address.getHostString(),
                                address.getPort());
                        if (!socketAddress.isUnresolved()) {
                            servers.add(new EquivalentAddressGroup(socketAddress));
                        }
                    });
                }

                // Update gRPC load balancer with the new set of server addresses.
                log.info("Updating client with controllers: {}", servers);
                this.resolverUpdater.onAddresses(servers, Attributes.EMPTY);

                // We have found at least one controller endpoint. Repeat discovery after the regular schedule.
                nextScheduleTimeMS = REFRESH_INTERVAL_MS;
            } catch (Throwable e) {
                // Catching all exceptions here since this method should never exit without rescheduling the discovery.
                if (e instanceof StatusRuntimeException) {
                    this.resolverUpdater.onError(((StatusRuntimeException) e).getStatus());
                } else {
                    this.resolverUpdater.onError(Status.UNKNOWN);
                }
                log.warn("Failed to construct controller endpoint list: ", e);

                // Attempt retry with a lower timeout on failures to improve re-connectivity time.
                nextScheduleTimeMS = FAILURE_RETRY_TIMEOUT_MS;
            } finally {
                // We avoid all blocking calls under a lock.
                updateSchedule(nextScheduleTimeMS);
            }
        }

        @Synchronized
        private void updateSchedule(final long nextScheduleTimeMS) {
            if (!shutdown) {
                log.info("Rescheduling ControllerNameResolver task for after {} ms", nextScheduleTimeMS);
                this.scheduledFuture = this.scheduledExecutor.schedule(
                        this::getControllers, nextScheduleTimeMS, TimeUnit.MILLISECONDS);

                // Record the last discovery time.
                this.lastUpdateTimeMS = System.currentTimeMillis();
            }
        }
    }
}
