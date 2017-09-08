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
import io.pravega.common.health.processor.HealthRequestProcessor;
import io.pravega.common.health.NoSuchHealthProcessor;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class HealthRequestProcessorImpl extends HealthRequestProcessor {
    public static final char PROCESSOR_SEPARATOR = '/';

    public HealthRequestProcessorImpl(HealthReporter root) {
        super(root);
    }

    @Override
    public final void processHealthRequest(OutputStream writer, String target, String cmd) throws IOException {
        if (root.getID().equals(target) || target.startsWith(root.getID() + PROCESSOR_SEPARATOR)) {
            root.executeHealthRequest(cmd, target, new DataOutputStream(writer));
        } else {
            throw new NoSuchHealthProcessor(target);
        }
    }


}
