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
import java.util.Arrays;
import java.util.List;

/**
 * Sample command. TODO: remove soon.
 */
class SampleCommand extends CommandBase {
    SampleCommand(List<String> args) {
        super(args);
    }

    @Override
    protected void validateArgs(List<String> args) {
        Preconditions.checkArgument(args.size() == 2, "Unexpected number of args.");
    }

    @Override
    void execute() {

    }

    static CommandDescriptor getDescriptor() {
        return new CommandDescriptor("comp", "cmd",
                "Does absolutely nothing",
                Arrays.asList(new ArgDescriptor("a", "Letter A"),
                        new ArgDescriptor("b", "Letter B")));
    }
}
