/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import java.io.DataOutput;
import java.io.IOException;

/**
 * The interface for all things that go over the Wire. (This is the basic element of the network protocol)
 */
public interface WireCommand {
    WireCommandType getType();

    void writeFields(DataOutput out) throws IOException;
}
