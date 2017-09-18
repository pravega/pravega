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

import com.google.common.base.Strings;
import io.pravega.common.health.HealthReporter;
import io.pravega.common.health.NoSuchHealthProcessor;
import io.pravega.common.health.processor.HealthRequestProcessor;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
        DataOutputStream opStr = new DataOutputStream(writer);
        if (!Strings.isNullOrEmpty(target)) {
            HealthReporter reporter = reporterMap.get(target);
            if (reporter != null) {
                reporter.execute(cmd, opStr);
            } else {
                throw new NoSuchHealthProcessor(target);
            }
        } else {
            switch (cmd) {
                case "tgts":
                    reporterMap.forEach( (id, reporter) -> {
                        try {
                            opStr.writeBytes(id + " { ");
                        Arrays.stream(reporter.listCommands()).forEach(cmds -> {
                            try {
                                opStr.writeBytes(cmds + " ");
                            } catch (IOException e) {
                                log.warn("Exception while writing");
                            }
                        });
                            opStr.writeBytes(" } \n");
                        } catch (IOException e) {
                            log.warn("Error while writing health");
                        }
                    });
                    break;
                    default:
                        log.warn("Unknown cmd {}", cmd);
            }
        }
    }


}
