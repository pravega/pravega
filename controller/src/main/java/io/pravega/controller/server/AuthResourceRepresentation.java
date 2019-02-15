/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.common.Exceptions;

/**
 * A utility class with methods for preparing string representations of auth-protected resources.
 */
public class AuthResourceRepresentation {

    public static String ofScopes() {
        return "/";
    }

    public static String ofAScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    public static String ofStreams(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    public static String ofAStream(String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s", ofStreams(scopeName), streamName);
    }

    public static String ofReaderGroups(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    public static String ofAReaderGroup(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s", ofReaderGroups(scopeName), readerGroupName);
    }
}