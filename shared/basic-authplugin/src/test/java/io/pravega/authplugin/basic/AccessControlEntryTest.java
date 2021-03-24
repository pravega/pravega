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
package io.pravega.authplugin.basic;

import io.pravega.auth.AuthHandler;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AccessControlEntryTest {

    @Test
    public void fromStringWithNoSpaces() {
        AccessControlEntry ace = AccessControlEntry.fromString("prn::*,READ");
        assertNotNull(ace);
        assertEquals("prn::.*", ace.getResourcePattern());
        assertEquals(AuthHandler.Permissions.READ, ace.getPermissions());
    }

    @Test
    public void fromStringWithEmptySpaces() {
        AccessControlEntry ace = AccessControlEntry.fromString(" prn::* , READ "); // with spaces
        assertNotNull(ace);
        assertEquals("prn::.*", ace.getResourcePattern());
        assertEquals(AuthHandler.Permissions.READ, ace.getPermissions());
    }

    @Test
    public void fromStringInstantiatesNullObjectIfInputIsBlank() {
        assertNull(AccessControlEntry.fromString(null));
        assertNull(AccessControlEntry.fromString(""));
        assertNull(AccessControlEntry.fromString("  "));
    }

    @Test
    public void fromStringInstantiatesNullObjectIfInputIsInvalid() {
        assertNull(AccessControlEntry.fromString("ABC")); // permission missing
        assertNull(AccessControlEntry.fromString(",READ")); // resource string/pattern missing
        assertNull(AccessControlEntry.fromString("prn::/,INVALID_PERMISSION")); // invalid permission
        assertNull(AccessControlEntry.fromString(",,READ")); // resource string/pattern missing
    }
}
