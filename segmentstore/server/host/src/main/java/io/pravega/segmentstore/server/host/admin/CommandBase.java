/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Base class for any command to execute from the Admin tool.
 */
abstract class CommandBase {
    @Getter(AccessLevel.PROTECTED)
    private final List<String> args;

    /**
     * Creates a new instance of the CommandBase class and validates arguments.
     *
     * @param args The arguments for the command.
     */
    CommandBase(List<String> args) {
        validateArgs(args);
        this.args = Preconditions.checkNotNull(args, "args");
    }

    /**
     * When overridden in a derived class, validates that the given arguments are correct.
     *
     * @param args The arguments to validate.
     */
    protected abstract void validateArgs(List<String> args);

    /**
     * Executes the command with the arguments passed in via the Constructor.
     */
    abstract void execute();

    /**
     * Describes an argument.
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    @Getter
    static class ArgDescriptor {
        private final String name;
        private final String description;
    }

    /**
     * Describes a Command.
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    @Getter
    static class CommandDescriptor {
        private final String component;
        private final String name;
        private final String description;
        private final List<ArgDescriptor> args;
    }
}
