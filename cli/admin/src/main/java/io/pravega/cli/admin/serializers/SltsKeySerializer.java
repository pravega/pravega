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
package io.pravega.cli.admin.serializers;

import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SltsKeySerializer extends AbstractSerializer {
    @Override
    public String getName() {
        return "SLTS";
    }

    @Override
    public ByteBuffer serialize(String value) {
        return ByteBuffer.wrap(value.getBytes(Charsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return StandardCharsets.UTF_8.decode(serializedValue).toString();
    }
}
