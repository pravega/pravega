/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server;

import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.protocol.netty.WireCommandType;
import lombok.Getter;

/**
 * Wire command failed exception.
 */

public class WireCommandFailedException extends RuntimeException implements RetryableException {

    public enum Reason {
        ConnectionDropped,
        ConnectionFailed,
        UnknownHost,
        PreconditionFailed,
        AuthFailed,
        SegmentDoesNotExist,
        TableSegmentNotEmpty,
        TableKeyDoesNotExist,
        TableKeyBadVersion,
    }

    private final WireCommandType type;
    @Getter
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
