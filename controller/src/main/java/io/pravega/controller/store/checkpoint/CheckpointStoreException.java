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
package io.pravega.controller.store.checkpoint;

import io.pravega.controller.server.ControllerServerException;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Controller store exception.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CheckpointStoreException extends ControllerServerException {

    public enum Type {
        Unknown,
        NodeExists,
        NoNode,
        NodeNotEmpty,
        Active,
        Sealed,
        Connectivity
    }

    private final Type type;

    public CheckpointStoreException(Throwable t) {
        super(t);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, Throwable t) {
        super(t);
        this.type = type;
    }

    public CheckpointStoreException(String message) {
        super(message);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, String message) {
        super(message);
        this.type = type;
    }

    public CheckpointStoreException(String message, Throwable t) {
        super(message, t);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, String message, Throwable t) {
        super(message, t);
        this.type = type;
    }
}
