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

/**
 * Represents an hierarchical entity which can report its health.
 * Each health reporter should have an unique id under its parents.
 */
public abstract class HealthReporter {
    /**
     * Gets the id of the reporter.
     * @return id
     */
    public abstract String getID();

    /**
     * Adds a child with a given id to the list of reporters.
     * @param childId id of the child to be inserted.
     * @param child the reporter object.
     */
    public abstract void addChild(String childId, HealthReporter child);

    /**
     * Gets a list of children.
     * @return
     */
    public abstract String[] listChildren();

    /**
     * Returns a reporter with a given id.
     * @param childId id of the reporter.
     * @return reporter with the given id.
     */
    public abstract HealthReporter getChild(String childId);

    /**
     * Removed a reporter with a given id.
     * @param childId id of the reporter to be removed.
     */
    public abstract void removeChild(String childId);

    /**
     * Executes a given command on the childnode/self.
     * @param cmd   The cmd to be executed.
     * @param out The output of the command is written to this stream.
     * @throws IOException exception while writing to the stream.
     */
    public abstract void executeHealthRequest(String cmd, DataOutputStream out) throws IOException;

    /**
     * Lists all the commands supported by this processor.
     * @return
     */
    public abstract String[] listCommands();

    /**
     * Executes a given healthcommand on this node.
     * @param cmd the command to be executed.
     * @param out output of the health command is written to this stream.
     * @throws IOException exception while writing to the stream.
     */
    public abstract void execute(String cmd, DataOutputStream out) throws IOException;

}
