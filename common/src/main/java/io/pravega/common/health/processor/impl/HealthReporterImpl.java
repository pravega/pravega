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
import io.pravega.common.health.NoSuchHealthCommand;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;



public abstract class HealthReporterImpl extends HealthReporter {
    private final String id;
    private final ConcurrentMap<String, HealthReporter> children;
    private List<String> commands;

    public HealthReporterImpl(String id, String[] commands) {
        this.id = id;
        this.children = new ConcurrentHashMap<String, HealthReporter>();
        this.commands = Arrays.asList(commands);
    }

    @Override
    public String getID() {
        return this.id;
    }

    @Override
    public void addChild(String childId, HealthReporter child) {
        this.children.put(childId, child);
    }

    @Override
    public void removeChild(String childId) {
        this.children.remove(childId);
    }

    @Override
    public final HealthReporter getChild(String childId) {
        return children.get(childId);
    }

    @Override
    public final void executeHealthRequest(String cmd, DataOutputStream out) throws IOException, NoSuchHealthCommand {
        if (this.commands.contains(cmd)) {
            this.execute(cmd, out);
        } else {
            throw new NoSuchHealthCommand(cmd);
        }
    }


    @Override
    public String[] listChildren() {
        return (String[]) children.keySet().toArray();
    }

    @Override
    public String[] listCommands() {
        return (String[]) this.commands.toArray();
    }

    public abstract void execute(String cmd, DataOutputStream out) throws IOException;
}
