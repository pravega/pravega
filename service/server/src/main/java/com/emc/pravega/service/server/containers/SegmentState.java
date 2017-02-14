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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.logs.operations.AttributeSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

/**
 * Current state of a segment. Objects of this class can be serialized/deserialized to/from a State Store.
 */
public class SegmentState {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    @Getter
    private final String segmentName;
    @Getter
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    SegmentState(SegmentProperties segmentProperties) {
        this.segmentName = segmentProperties.getName();
        this.attributes = segmentProperties.getAttributes();
    }

    private SegmentState(String segmentName, Map<UUID, Long> attributes) {
        this.segmentName = segmentName;
        this.attributes = attributes;
    }

    //endregion

    //region Serialization

    public void serialize(DataOutputStream target) throws IOException {
        target.writeByte(SERIALIZATION_VERSION);
        target.writeUTF(segmentName);
        AttributeSerializer.serialize(this.attributes, target);
    }

    public static SegmentState deserialize(DataInputStream source) throws IOException {
        byte version = source.readByte();
        if (version == SERIALIZATION_VERSION) {
            String segmentName = source.readUTF();
            Map<UUID, Long> attributes = AttributeSerializer.deserialize(source);
            return new SegmentState(segmentName, attributes);
        } else {
            throw new IOException(String.format("Unsupported version: %d.", version));
        }
    }

    //endregion
}
