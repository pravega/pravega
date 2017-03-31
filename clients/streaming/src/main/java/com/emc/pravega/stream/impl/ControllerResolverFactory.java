/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.google.common.base.Splitter;
import io.grpc.Attributes;
import io.grpc.NameResolver;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;

/**
 * gRPC Factory for resolving controller host ips and ports.
 */
public class ControllerResolverFactory extends NameResolver.Factory {

    private final static String SCHEME = "pravega";

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }
        return new ControllerNameResolver(targetUri, params);
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    private static class ControllerNameResolver extends NameResolver {

        private final String authority;
        private final List<String> BootstrapServers;

        public ControllerNameResolver(final URI targetUri, final Attributes params) {
            this.authority = targetUri.getAuthority();
            Splitter.on(","). targetUri.getPath();

        }

        @Override
        public String getServiceAuthority() {
            return null;
        }

        @Override
        public void start(Listener listener) {

        }

        @Override
        public void shutdown() {

        }
    }
}
