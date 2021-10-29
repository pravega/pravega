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
package io.pravega.cli.admin.serializers.controller;

import io.pravega.cli.admin.serializers.AbstractSerializer;

import java.nio.ByteBuffer;

public class SealedSegmentsMapShardSerializer extends AbstractSerializer {

    public

    @Override
    public String getName() {
        return "SealedSegmentsMapShard";
    }

    @Override
    public ByteBuffer serialize(String value) {
        return null;
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return null;
    }
}
