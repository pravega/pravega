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
import java.util.Arrays;
import java.util.List;

/**
 * Represents an hierarchical entity which can report its health.
 * Each health reporter should have an unique id under its parents.
 */
public abstract class HealthReporter {
    private final String id;
    private List<String> commands;

    public HealthReporter(String id, String[] commands) {
        this.id = id;
        this.commands = Arrays.asList(commands);
    }

    public String getID() {
        return this.id;
    }

    /**
     * Lists all the commands supported by this processor.
     * @return list of supported commands.
     */
    public String[] listCommands() {
        return (String[]) commands.toArray();
    }

    /**
     * Executes a given healthcommand on this node.
     * @param cmd the command to be executed.
     * @param out output of the health command is written to this stream.
     * @throws HealthReporterException exception while reporting health.
     */
    public abstract void execute(String cmd, DataOutputStream out);

}
