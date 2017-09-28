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
import io.pravega.common.health.NoSuchHealthCommand;
import io.pravega.common.health.NoSuchHealthProcessor;
import io.pravega.common.health.processor.HealthRequestProcessor;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HealthRequestProcessorImpl extends HealthReporter implements HealthRequestProcessor {
    private final ConcurrentHashMap<String, HealthReporter> reporterMap = new ConcurrentHashMap<>();

    public HealthRequestProcessorImpl() {
        super();
    }

    @Override
    public Map<String, Consumer<DataOutputStream>> createHandlers() {
        Map<String, Consumer<DataOutputStream>> handlersMap = new HashMap<>();
        handlersMap.put("tgts", this::handleTgts);
        return handlersMap;
    }

    private void handleTgts(DataOutputStream dataOutputStream) {
        reporterMap.forEach( (id, reporter) -> {
            try {
                dataOutputStream.writeBytes(id + " { ");
                reporter.listCommands().forEach(cmds -> {
                    try {
                        dataOutputStream.writeBytes(cmds + " ");
                    } catch (IOException e) {
                        log.warn("Exception while writing");
                    }
                });
                dataOutputStream.writeBytes(" } \n");
            } catch (IOException e) {
                log.warn("Error while writing health");
            }
        });
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
                Consumer<DataOutputStream> handler = reporter.getHandlers().get(cmd);
                if (handler != null) {
                    handler.accept(new DataOutputStream(writer));
                } else {
                    throw new NoSuchHealthCommand(cmd);
                }
            } else {
                throw new NoSuchHealthProcessor(target);
            }
        }
    }


}
