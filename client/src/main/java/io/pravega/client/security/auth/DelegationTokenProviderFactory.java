/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.Controller;
import lombok.NonNull;

/**
 * Factory class for {@link DelegationTokenProvider} instances.
 */
public class DelegationTokenProviderFactory {

    /**
     * Creates a {@link DelegationTokenProvider} instance with empty token. Intended to be Used for testing only.
     *
     * @return a new {@link DelegationTokenProvider} instance
     */
    @VisibleForTesting
    public static DelegationTokenProvider createWithEmptyToken() {
        return new EmptyTokenProviderImpl();
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with null delegation token.
     *
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller}, {@code scopeName} or {@code streamName} is null
     * @throws IllegalArgumentException if {@code scopeName} or {@code streamName} is empty
     */
    @VisibleForTesting
    public static DelegationTokenProvider create(Controller controller, String scopeName, String streamName) {
        return create(null, controller, scopeName, streamName);
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with null delegation token.
     *
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param segment the {@link Segment}, for which a delegation token is to be obtained
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller} or {@code segment} is null
     */
    public static DelegationTokenProvider create(Controller controller, Segment segment) {
        return create(null, controller, segment);
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with the specified delegation token.
     *
     * @param delegationToken an existing delegation token to populate the {@link DelegationTokenProvider} instance with
     * @param controller  the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param segment the {@link Segment}, for which a delegation token is to be obtained
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller} or {@code segment} is null
     */
    public static DelegationTokenProvider create(String delegationToken, @NonNull Controller controller,
                                                 @NonNull Segment segment) {
         return create(delegationToken, controller, segment.getScope(), segment.getStreamName());
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance of an appropriate type.
     *
     * @param delegationToken an existing delegation token to populate the {@link DelegationTokenProvider} instance with.
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller if/when
     *                   the token expires or is nearing expiry
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @return a new {@link DelegationTokenProvider} instance
     */
    public static DelegationTokenProvider create(String delegationToken, Controller controller, String scopeName,
                                                 String streamName) {
        if (delegationToken == null) {
            return new JwtTokenProviderImpl(controller, scopeName, streamName);
        } else if (delegationToken.equals("")) {
            return new EmptyTokenProviderImpl();
        } else if (delegationToken.split("\\.").length == 3) {
            return new JwtTokenProviderImpl(delegationToken, controller, scopeName, streamName);
        } else {
            return new StringTokenProviderImpl(delegationToken);
        }
    }
}
