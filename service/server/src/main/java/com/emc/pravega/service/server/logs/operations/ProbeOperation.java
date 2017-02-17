/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.server.logs.SerializationException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * No-op operation that can be used as an "operation barrier". This can be added to the Log and when it completes, the
 * caller knows that all operations up to, and including it, have been completed (successfully or not). This operation
 * cannot be serialized or recovered.
 */
public class ProbeOperation extends Operation {
    public static final byte OPERATION_TYPE = 0;

    public ProbeOperation() {
        super();
    }

    @Override
    public boolean canSerialize() {
        // We cannot (and should not) process this operation in the log. It serves no real purpose except as a control
        // op (see class-level doc).
        return false;
    }

    @Override
    protected byte getOperationType() {
        return OPERATION_TYPE;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized.");
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be deserialized.");
    }
}
