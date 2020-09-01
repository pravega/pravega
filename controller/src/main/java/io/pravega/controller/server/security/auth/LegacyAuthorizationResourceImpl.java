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
 * A legacy implementation that constructs resource strings in the old format.
 */
public final class LegacyAuthorizationResourceImpl implements AuthorizationResource {

    public String ofScopes() {
        return "/";
    }

    public String ofScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    public String ofStreamsInScope(String scopeName) {
        return Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
    }

    public String ofStreamInScope(String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s", ofStreamsInScope(scopeName), streamName);
    }

    public String ofReaderGroupsInScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    public String ofReaderGroupInScope(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s", ofReaderGroupsInScope(scopeName), readerGroupName);
    }

    @Override
    public String ofWatermarkInScope(String scopeName, String watermarkName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    public String ofKeyValueTableInScope(String scopeName, String kvtName) {
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTableName");
        return String.format("%s/_kvtable/%s", ofStreamsInScope(scopeName), kvtName);
    }
}
