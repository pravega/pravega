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
    private static final char PROCESSOR_SEPARATOR = '/';

    public HealthRequestProcessorImpl(HealthReporter root) {
        super(root);
    }

    @Override
    public final void ProcessHealthRequest(OutputStream writer, String target, String cmd) throws IOException {
            findProcessor(target).executeHealthRequest(cmd, new DataOutputStream(writer));
    }

    private HealthReporter findProcessor(String target) throws NoSuchHealthProcessor {
        HealthReporter looper = root;
        int separatorIndex = -1;
        do {
            separatorIndex = target.indexOf(PROCESSOR_SEPARATOR);
            if (separatorIndex == -1) {
                if (looper.getID().equals(target)) {
                    return looper;
                } else {
                    throw new NoSuchHealthProcessor(target);
                }
            } else {
                looper = looper.getChild(target.substring(0, separatorIndex));
                target = target.substring(separatorIndex);
            }
        } while (separatorIndex != -1);
        throw new NoSuchHealthProcessor(target);
    }
}
