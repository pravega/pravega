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
import java.util.concurrent.ConcurrentHashMap;

public class HealthRequestProcessorImpl extends HealthRequestProcessor {
    private final ConcurrentHashMap<String, HealthReporter> reporterMap = new ConcurrentHashMap<>();

    public HealthRequestProcessorImpl() {
        super();
    }

    @Override
    public void registerHealthProcessor(String id, HealthReporter reporter) {
        reporterMap.putIfAbsent(id, reporter);
    }

    @Override
    public final void processHealthRequest(OutputStream writer, String target, String cmd) throws IOException {
        HealthReporter reporter = reporterMap.get(target);
        if (reporter != null) {
            reporter.execute(cmd, new DataOutputStream(writer));
        } else {
            throw new NoSuchHealthProcessor(target);
        }
    }


}
