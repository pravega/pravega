/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.health.processor.impl;

import io.pravega.common.health.HealthReporter;

/**
 * A HealthRequestProcessor driven from a socket stream.
 */
public class SocketStreamHealthRequestProcessorImpl extends HealthRequestProcessorImpl {
    public SocketStreamHealthRequestProcessorImpl(HealthReporter root, int port) {
        super(root);
    }
}
