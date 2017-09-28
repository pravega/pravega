/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.health;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a hierarchical entity which can report its health.
 * Each health reporter should have an unique id under its parents.
 */
@Slf4j
public abstract class HealthReporter {
    @Getter
    private final Map<String, Consumer<DataOutputStream>> handlers;

    public HealthReporter() {
        this.handlers = createHandlers();
    }

    /**
     * Lists all the commands supported by this processor.
     * @return list of supported commands.
     */
    public Set<String> listCommands() {
        return handlers.keySet();
    }

    //region health reporter executor

    public Map<String, Consumer<DataOutputStream>> createHandlers() {
        Map<String, Consumer<DataOutputStream>> handlersMap = new HashMap<>();
        handlersMap.put("ruok", this::handleRuok);
        return handlersMap;
    }

    //endregion
    public void handleRuok(DataOutputStream str) {
        try {
            str.writeBytes("IMOK");
        } catch (IOException e) {
            log.warn("Error handling command");
        }
    }
}
