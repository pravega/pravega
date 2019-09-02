/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;

/**
 * Command arguments.
 */
@Getter
public class CommandArgs {
    private final List<String> args;
    private final AdminCommandState state;

    /**
     * Creates a new instance of the CommandArgs class.
     *
     * @param args  The actual arguments to the command.
     * @param state The shared AdminCommandState to pass to the command.
     */
    public CommandArgs(List<String> args, AdminCommandState state) {
        this.args = Preconditions.checkNotNull(args, "args");
        this.state = Preconditions.checkNotNull(state, "state");
    }
}
