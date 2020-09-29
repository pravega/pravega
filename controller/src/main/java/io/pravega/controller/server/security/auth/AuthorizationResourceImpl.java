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

/**
 * The main implementation of the {@link AuthorizationResource} class.
 */
public class AuthorizationResourceImpl implements AuthorizationResource {
    public static final String DOMAIN_PART_SUFFIX = "prn::";
    private static final String TAG_SCOPE = "scope";
    private static final String TAG_STREAM = "stream";
    private static final String TAG_READERGROUP = "reader-group";
    private static final String TAG_KEYVALUETABLE = "key-value-table";

    private static final String ROOT_RESOURCE = String.format("%s/", DOMAIN_PART_SUFFIX);

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
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_STREAM, streamName);
    }

    @Override
    public String ofReaderGroupsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    @Override
    public String ofReaderGroupInScope(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_READERGROUP, readerGroupName);
    }

    @Override
    public String ofKeyValueTableInScope(String scopeName, String keyValueTableName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(keyValueTableName, "keyValueTableName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_KEYVALUETABLE, keyValueTableName);
    }
}
