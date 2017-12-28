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

import com.google.auth.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PravegaCredsWrapper extends Credentials {
    private final PravegaCredentials creds;

    public PravegaCredsWrapper(PravegaCredentials creds) {
        this.creds = creds;
    }

    @Override
    public String getAuthenticationType() {
        return creds.getAuthenticationType();
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
        Map<String, String> metadata = creds.getAuthHeaders();
        metadata.put("method", creds.getAuthenticationType());

        return metadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> {
                    List<String> list = new ArrayList<>();
                    list.add(e.getValue());
                    return list;
                }));
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

    }
}
