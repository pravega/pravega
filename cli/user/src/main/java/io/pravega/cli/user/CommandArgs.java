/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;

import io.pravega.cli.user.config.InteractiveConfig;
import lombok.Data;
import lombok.NonNull;

import java.util.List;

/**
 * Command arguments.
 */
@Data
public class CommandArgs {
    @NonNull
    private final List<String> args;
    @NonNull
    private final InteractiveConfig config;

    @Override
    public String toString() {
        return String.format("%s; Config={%s}", String.join(", ", this.args), this.config);
    }
}
