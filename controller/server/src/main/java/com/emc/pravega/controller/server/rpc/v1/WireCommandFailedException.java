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
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.controller.RetryableException;

/**
 * Wire command failed exception.
 */
public class WireCommandFailedException extends RetryableException {

    public enum Reason {
        ConnectionDropped,
        ConnectionFailed,
        UnknownHost,
        PreconditionFailed,
    }

    private final WireCommandType type;
    private final Reason reason;

    public WireCommandFailedException(Throwable cause, WireCommandType type, Reason reason) {
        super(cause);
        this.type = type;
        this.reason = reason;
    }

    public WireCommandFailedException(WireCommandType type, Reason reason) {
        super(String.format("WireCommandFailed with type %s reason %s", type.toString(), reason.toString()));
        this.type = type;
        this.reason = reason;
    }
}
