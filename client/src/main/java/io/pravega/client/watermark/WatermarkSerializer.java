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
package io.pravega.client.watermark;

import io.pravega.client.stream.Serializer;
import io.pravega.shared.watermarks.Watermark;

import java.nio.ByteBuffer;

public class WatermarkSerializer implements Serializer<Watermark> {
    @Override
    public ByteBuffer serialize(Watermark value) {
        return value.toByteBuf();
    }

    @Override
    public Watermark deserialize(ByteBuffer serializedValue) {
        return Watermark.fromByteBuf(serializedValue);
    }
}
