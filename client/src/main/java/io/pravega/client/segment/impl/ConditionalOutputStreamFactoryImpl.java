/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.Controller;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConditionalOutputStreamFactoryImpl implements ConditionalOutputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;
    
    @Override
    public ConditionalOutputStream createConditionalOutputStream(Segment segment, String delegationToken) {
        return new ConditionalOutputStreamImpl(UUID.randomUUID(), segment, controller, cf, delegationToken);
    }

}
