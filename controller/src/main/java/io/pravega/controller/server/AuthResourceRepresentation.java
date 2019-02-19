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
 * <p>
 * Background:
 * <p>
 * In general, authorization is about granting a <i>subject</i> access to perform a particular <i>action</i>
 * on an <b>object/resource</b>.
 * <p>
 * In Pravega,
 * <ul>
 *     <li>A subject is represented as an instance of type {@link java.security.Principal}.</li>
 *     <li>An action is represented as an element of enum type {@Link io.pravega.auth.AuthHandler.Permissions}.</li>
 *     <li>An object is represented by an instance of <i>this</i> class.</li>
 * </ul>
 */
public final class AuthResourceRepresentation {

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the collection of scopes
     * in the system.
     *
     * @return a string representing the collections of scopes in the system
     */
    public static String ofScopes() {
        return "/";
    }

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the specified scope.
     *
     * @param scopeName the name of the scope
     * @return a string representing the scope with the specified name
     * @throws NullPointerException if {@code scopeName} is null
     * @throws IllegalArgumentException if {@code scopeName} is empty
     */
    public static String ofAScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the collection of streams
     * within the specified scope.
     *
     * @param scopeName the name of the scope
     * @return a string representing the collection of streams within the scope with the specified name
     * @throws NullPointerException if {@code scopeName} is null
     * @throws IllegalArgumentException if {@code scopeName} is empty
     */
    public static String ofStreams(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the specified stream within
     * the specified scope.
     *
     * @param scopeName the name of the scope
     * @param streamName the name of the stream
     * @return a string representing the specified stream within the specified scope
     * @throws NullPointerException if {@code scopeName} or {@code streamName} are null
     * @throws IllegalArgumentException if {@code scopeName} or {@code streamName} are empty
     */
    public static String ofAStream(String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s", ofStreams(scopeName), streamName);
    }

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the collection of reader
     * groups within the specified scope.
     *
     * @param scopeName the name of the scope
     * @return a string representing the specified the collection of reader groups
     * @throws NullPointerException if {@code scopeName} is null
     * @throws IllegalArgumentException if {@code scopeName} is empty
     */
    public static String ofReaderGroups(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return scopeName;
    }

    /**
     * Creates a resource representation for use in authorization of actions pertaining to the specified reader group
     * within the specified scope.
     *
     * @param scopeName the name of the scope
     * @param readerGroupName the name of the reader group
     * @return a string representing the specified reader group
     * @throws NullPointerException if {@code scopeName} or {@code streamName} are null
     * @throws IllegalArgumentException if {@code scopeName} or {@code streamName} are empty
     */
    public static String ofAReaderGroup(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s", ofReaderGroups(scopeName), readerGroupName);
    }
}