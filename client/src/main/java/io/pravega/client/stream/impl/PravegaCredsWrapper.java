/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PravegaCredsWrapper extends com.google.auth.Credentials {
    private final Credentials creds;

    public PravegaCredsWrapper(Credentials creds) {
        this.creds = creds;
    }

    @Override
    public String getAuthenticationType() {
        return creds.getAuthenticationType();
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
        Map<String, String> metadata = creds.getAuthParameters();

        Map<String, List<String>> retVal = metadata.entrySet().stream().collect(
                Collectors.toMap(entry -> Credentials.AUTH_HANDLER_PREFIX + entry.getKey(),
                        e -> Collections.singletonList(e.getValue())));
        retVal.put(Credentials.AUTH_HANDLER_PREFIX + "method", Collections.singletonList(creds.getAuthenticationType()));
        return retVal;
    }

    @Override
    public boolean hasRequestMetadata() {
        return true;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
        return true;
    }

    @Override
    public void refresh() throws IOException {
        // All the Pravega credentials are purely map based. They are not supposed to be refreshed through this flow.
    }
}
