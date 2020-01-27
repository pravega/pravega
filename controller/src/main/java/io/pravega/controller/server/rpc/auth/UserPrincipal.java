/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import io.pravega.common.Exceptions;

import java.io.Serializable;
import java.security.Principal;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A {@code Principal} represents an identity (of a subject). This class implements
 * {@code Principal} and represents identity in the form of a user's name.
 */
@ToString
@EqualsAndHashCode
public class UserPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;

    /**
     * Constructs a {@code Principal} representing the given user name.
     *
     * @param name the user name
     */
    public UserPrincipal(String name) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        this.name = name;
    }

    /**
     * Returns the name of this {@code Principal}.
     *
     * @return the name of this {@code Principal}
     */
    @Override
    public String getName() {
        return name;
    }
}
