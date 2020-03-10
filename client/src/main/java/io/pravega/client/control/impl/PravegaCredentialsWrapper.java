/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.control.impl;

import io.pravega.auth.AuthConstants;
import io.pravega.client.stream.impl.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PravegaCredentialsWrapper extends com.google.auth.Credentials {
    private final Credentials credentials;

    public PravegaCredentialsWrapper(Credentials credentials) {
        this.credentials = credentials;
    }

    @Override
    public String getAuthenticationType() {
        return credentials.getAuthenticationType();
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
        String token = credentials.getAuthenticationToken();
        String credential = credentials.getAuthenticationType() + " " + token;
        return Collections.singletonMap(AuthConstants.AUTHORIZATION, Collections.singletonList(credential));
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
