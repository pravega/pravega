/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;

/**
 * Username/password credentials for basic authentication.
 *
 * @deprecated As of Pravega release 0.9, replaced by {@link io.pravega.shared.security.auth.DefaultCredentials}.
 */
@Deprecated
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_INTERFACE",
        justification = "Interface with same name retained for compatibility with older implementations")
@EqualsAndHashCode
public class DefaultCredentials implements Credentials {

    private static final long serialVersionUID = 1L;

    @EqualsAndHashCode.Exclude
    private final io.pravega.shared.security.auth.DefaultCredentials delegate;

    private final String token;

    public DefaultCredentials(String password, String userName) {
        delegate = new io.pravega.shared.security.auth.DefaultCredentials(password, userName);
        token = delegate.getAuthenticationToken();
    }

    @Override
    public String getAuthenticationType() {
        return delegate.getAuthenticationType();
    }

    @Override
    public String getAuthenticationToken() {
        return token;
    }
}
