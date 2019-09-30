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

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;

/**
 * Creates a {@link DelegationTokenProvider} instance.
 */
public class DelegationTokenProviderFactory {

    /**
     * Creates a {@link DelegationTokenProvider} instance with empty token.
     *
     * @return a new {@link DelegationTokenProvider} instance
     */
    public static DelegationTokenProvider createWithEmptyToken() {
        return new DelegationTokenProviderImpl("");
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
    public static DelegationTokenProvider create(Controller controller, String scopeName, String streamName) {
        Preconditions.checkNotNull(controller, "Argument controller is null");
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return new DelegationTokenProviderImpl(controller, scopeName, streamName);
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
     * Creates a {@link DelegationTokenProvider} instance with null delegation token.
     *
     * @param delegationToken an existing delegation token to populate the {@link DelegationTokenProvider} instance with
     * @param controller  the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param segment the {@link Segment}, for which a delegation token is to be obtained
     * @return a new {@link DelegationTokenProvider} instance
     */
    public static DelegationTokenProvider create(String delegationToken, Controller controller,
                                                 Segment segment) {
        Preconditions.checkNotNull(controller, "Argument controller is null");
        Preconditions.checkNotNull(segment, "Argument segment is null");
        return new DelegationTokenProviderImpl(delegationToken, controller, segment.getScope(), segment.getStreamName());
    }
}
