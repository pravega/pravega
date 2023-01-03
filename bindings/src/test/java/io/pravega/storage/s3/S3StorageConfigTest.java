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
package io.pravega.storage.s3;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S3StorageConfigTest {

    @Test
    public void testDefaultS3Config() {
        ConfigBuilder<S3StorageConfig> builder = S3StorageConfig.builder();
        builder.with(Property.named("configUri"), "http://127.0.0.1:9020")
                .with(Property.named("bucket"), "testBucket")
                .with(Property.named("prefix"), "testPrefix");
        S3StorageConfig config = builder.build();
        assertEquals("testBucket", config.getBucket());
        assertEquals("testPrefix/", config.getPrefix());
        assertEquals("us-east-1", config.getRegion());
        assertEquals(false, config.isShouldOverrideUri());
    }

    @Test
    public void testConstructS3Config() {
        ConfigBuilder<S3StorageConfig> builder = S3StorageConfig.builder();
        builder.with(Property.named("connect.config.uri"), "http://example.com")
                .with(Property.named("bucket"), "testBucket")
                .with(Property.named("prefix"), "testPrefix")
                .with(Property.named("connect.config.region"), "my-region")
                .with(Property.named("connect.config.access.key"), "key")
                .with(Property.named("connect.config.secret.key"), "secret")
                .with(Property.named("connect.config.role"), "role")
                .with(Property.named("connect.config.uri.override"), true)
                .with(Property.named("connect.config.assumeRole.enable"), true);
        S3StorageConfig config = builder.build();
        assertEquals("testBucket", config.getBucket());
        assertEquals("testPrefix/", config.getPrefix());
        assertEquals("my-region", config.getRegion());
        assertEquals(true, config.isShouldOverrideUri());
        assertEquals("http://example.com", config.getS3Config());
        assertEquals( "my-region", config.getRegion());
        assertEquals( "key", config.getAccessKey());
        assertEquals( "secret", config.getSecretKey());
        assertEquals( "role", config.getUserRole());
        assertEquals( true, config.isAssumeRoleEnabled());
    }
}
