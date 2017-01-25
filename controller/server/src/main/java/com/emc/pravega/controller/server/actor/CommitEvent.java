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
package com.emc.pravega.controller.server.actor;

import com.emc.pravega.controller.actor.StreamEvent;
import com.emc.pravega.stream.Serializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang.NotImplementedException;

import java.util.UUID;

@Data
@AllArgsConstructor
public class CommitEvent implements StreamEvent {
    private final String scope;
    private final String stream;
    private final UUID txid;

    public byte[] getBytes() {
        throw new NotImplementedException();
    }

    public static Serializer<CommitEvent> getSerializer() {
        throw new NotImplementedException();
    }
}
