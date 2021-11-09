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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.controller.SubscribersSerializer.SUBSCRIBERS_SUBSCRIBERS;
import static org.junit.Assert.assertEquals;

public class SubscribersSerializerTest {

    @Test
    public void testSubscribersSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, SUBSCRIBERS_SUBSCRIBERS,
                convertCollectionToString(ImmutableSet.of("sub1"), s -> s));

        String userString = userGeneratedMetadataBuilder.toString();
        SubscribersSerializer serializer = new SubscribersSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscribersSerializerArgumentFailure() {
        String userString = "";
        SubscribersSerializer serializer = new SubscribersSerializer();
        serializer.serialize(userString);
    }
}
