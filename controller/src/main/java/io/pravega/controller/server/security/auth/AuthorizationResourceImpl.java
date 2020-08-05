/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth;

import io.pravega.common.Exceptions;

public class AuthorizationResourceImpl extends AuthorizationResource {
    public static final String DOMAIN_PART_SUFFIX = "prn::";
    private static final String TAG_SCOPE = "scope";
    private static final String TAG_STREAM = "stream";
    private static final String TAG_READERGROUP = "reader-group";

    private static final String ROOT_RESOURCE = String.format("%s/%s", DOMAIN_PART_SUFFIX, "/");

    @Override
    public String ofScopes() {
        return ROOT_RESOURCE;
    }

    @Override
    public String ofScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return String.format("%s/%s:%s", DOMAIN_PART_SUFFIX, TAG_SCOPE, scopeName);
    }

    @Override
    public String ofStreamsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    @Override
    public String ofStreamInScope(String scopeName, String streamName) {
        return null;
    }

    @Override
    public String ofReaderGroupsInScope(String scopeName) {
        return null;
    }

    @Override
    public String ofReaderGroupInScope(String scopeName, String readerGroupName) {
        return null;
    }

    @Override
    public String ofKeyValueTableInScope(String scopeName, String kvtName) {
        return null;
    }
}
