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
package io.pravega.controller.server.security.auth;

import io.pravega.auth.AuthHandler;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.test.common.AssertExtensions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamAuthParamsTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Test
    public void rejectsConstructionWhenInputIsInvalid() {
        AssertExtensions.assertThrows("Scope was null",
                () -> new StreamAuthParams(null, "_internalStream"),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Stream was null",
                () -> new StreamAuthParams("scope", null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void recognizesInternalStreams() {
        assertTrue(new StreamAuthParams("testscope", "_internalStream").isInternalStream());
        assertTrue(new StreamAuthParams("testscope", "_MARKteststream").isInternalStream());

        StreamAuthParams readerGroupStreamAuthParams = new StreamAuthParams("testscope", "_RGtestApp");
        assertTrue(readerGroupStreamAuthParams.isInternalStream());
        assertFalse(readerGroupStreamAuthParams.isStreamUserDefined());
    }

    @Test
    public void recognizesExternalStreams() {
        StreamAuthParams streamAuthParams = new StreamAuthParams("testscope", "teststream");
        assertFalse(streamAuthParams.isInternalStream());
        assertTrue(streamAuthParams.isStreamUserDefined());
    }

    @Test
    public void returnsAppropriatePermissionForRgStreamWhenConfigIsTrue() {
        StreamAuthParams params = new StreamAuthParams("scope", "_RGinternalStream", true);
        assertEquals(AuthHandler.Permissions.READ, params.requiredPermissionForWrites());
    }

    @Test
    public void returnsAppropriatePermissionForInternalStreamWhenConfigIsTrue() {
        StreamAuthParams params = new StreamAuthParams("scope", "_internalStream", true);
        assertEquals(AuthHandler.Permissions.READ_UPDATE, params.requiredPermissionForWrites());
    }

    @Test
    public void returnsReadUpdatePermissionWhenConfigIsFalse() {
        StreamAuthParams params = new StreamAuthParams("scope", "_internalStream", false);
        assertEquals(AuthHandler.Permissions.READ_UPDATE, params.requiredPermissionForWrites());
    }

    @Test
    public void isAccessOperationUnspecified() {
        assertTrue(new StreamAuthParams("scope", "stream", AccessOperation.UNSPECIFIED, false).isAccessOperationUnspecified());
    }

    @Test
    public void requestedPermissionForWatermarkStream() {
        StreamAuthParams params1 = new StreamAuthParams("testScope", NameUtils.getMarkStreamForStream("testStream"),
                AccessOperation.UNSPECIFIED, false);
        assertEquals(AuthHandler.Permissions.READ, params1.requiredPermissionForWrites());

        StreamAuthParams params2 = new StreamAuthParams("testscope", "_MARKteststream",
                AccessOperation.READ, false);
        assertEquals(AuthHandler.Permissions.READ, params2.requiredPermissionForWrites());

        StreamAuthParams params3 = new StreamAuthParams("testscope", "_MARKteststream",
                AccessOperation.READ_WRITE, false);
        assertEquals(AuthHandler.Permissions.READ, params3.requiredPermissionForWrites());
    }

    @Test
    public void returnsReadUpdatePermissionForExternalStreams() {
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                new StreamAuthParams("scope", "externalStream", true).requiredPermissionForWrites());
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                new StreamAuthParams("scope", "externalStream", false).requiredPermissionForWrites());
    }

    @Test
    public void resourceStringReturnsAppropriateRepresentation() {
        assertEquals("prn::/scope:testScope/stream:testExternalStream",
                new StreamAuthParams("testScope", "testExternalStream").resourceString());
        assertEquals("prn::/scope:testScope/stream:_requeststream",
                new StreamAuthParams("testScope", "_requeststream").resourceString());
        assertEquals("prn::/scope:testScope/reader-group:testRg",
                new StreamAuthParams("testScope", "_RGtestRg").resourceString());
    }

    @Test
    public void streamResourceStringReturnsStreamRepresentation() {
        assertEquals("prn::/scope:testScope/stream:testExternalStream",
                new StreamAuthParams("testScope", "testExternalStream").streamResourceString());
        assertEquals("prn::/scope:testScope/stream:_requeststream",
                new StreamAuthParams("testScope", "_requeststream").streamResourceString());
        assertEquals("prn::/scope:testScope/stream:_RGtestRg",
                new StreamAuthParams("testScope", "_RGtestRg").streamResourceString());
    }

    @Test
    public void requestedPermissionReturnsSpecifiedOrDefault() {
        // Default
        assertEquals(AuthHandler.Permissions.READ, new StreamAuthParams("testScope", "testExternalStream",
                AccessOperation.UNSPECIFIED, true).requestedPermission());

        // Specified
        assertEquals(AuthHandler.Permissions.READ_UPDATE, new StreamAuthParams("testScope", "testExternalStream",
                AccessOperation.READ_WRITE, true).requestedPermission());
    }
}
