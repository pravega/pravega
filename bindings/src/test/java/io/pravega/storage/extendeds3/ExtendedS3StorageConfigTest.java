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
package io.pravega.storage.extendeds3;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtendedS3StorageConfigTest {

    @Test
    public void testConstructS3Config() {
        ConfigBuilder<ExtendedS3StorageConfig> builder = ExtendedS3StorageConfig.builder();
        builder.with(Property.named("configUri"), "http://127.0.0.1:9020?namespace=sampleNamespace&identity=user&secretKey=password")
                .with(Property.named("bucket"), "testBucket")
                .with(Property.named("prefix"), "testPrefix");
        ExtendedS3StorageConfig config = builder.build();
        assertEquals("user", config.getAccessKey());
        assertEquals("password", config.getSecretKey());
        assertEquals("testBucket", config.getBucket());
        assertEquals("testPrefix/", config.getPrefix());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        ConfigBuilder<ExtendedS3StorageConfig> builder = ExtendedS3StorageConfig.builder();
        builder.with(Property.named("configUri"), "http://localhost:9020?namespace=sampleNamespace&identity=&secretKey=password")
                .with(Property.named("bucket"), "testBucket")
                .with(Property.named("prefix"), "testPrefix");
        ExtendedS3StorageConfig config = builder.build();
    }

    @Test (expected = NullPointerException.class)
    public void testMissingSecretKey() {
        ConfigBuilder<ExtendedS3StorageConfig> builder = ExtendedS3StorageConfig.builder();
        builder.with(Property.named("configUri"), "http://localhost:9020?namespace=sampleNamespace&identity=user")
                .with(Property.named("bucket"), "testBucket")
                .with(Property.named("prefix"), "testPrefix");
        ExtendedS3StorageConfig config = builder.build();
    }
}
