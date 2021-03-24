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
package io.pravega.shared.security.auth;

import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PasswordAuthHandlerInputTest {

    @Test
    public void ctorRejectsNullOrEmptyInput() {
        // Null input
        AssertExtensions.assertThrows(NullPointerException.class, () -> new PasswordAuthHandlerInput(null, ".non-null"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> new PasswordAuthHandlerInput("non-null", null));

        // Empty input
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> new PasswordAuthHandlerInput("", ".non-null"));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> new PasswordAuthHandlerInput("non-null", ""));
    }

    @Test
    public void createAndDelete() {
        PasswordAuthHandlerInput objectUnderTest = new PasswordAuthHandlerInput("PasswordAuthHandlerInputTest.createAndDelete", ".txt");
        assertTrue(objectUnderTest.getFile().exists());
        objectUnderTest.close();
        assertFalse(objectUnderTest.getFile().exists());
    }

    @SneakyThrows
    @Test
    public void postingEntries() {
        @Cleanup
        PasswordAuthHandlerInput objectUnderTest = new PasswordAuthHandlerInput("PasswordAuthHandlerInputTest.postingEntries", ".txt");
        String encryptedPassword = StrongPasswordProcessor.builder().build().encryptPassword("some_password");

        long initialFileSize = objectUnderTest.getFile().length();
        objectUnderTest.postEntry(PasswordAuthHandlerInput.Entry.of("admin", encryptedPassword, "prn::*,READ_UPDATE;"));
        assertTrue(objectUnderTest.getFile().length() > initialFileSize);
    }

    @SneakyThrows
    @Test
    public void postingNullOrEmptyEntriesIsRejected() {
        @Cleanup
        PasswordAuthHandlerInput objectUnderTest = new PasswordAuthHandlerInput("PasswordAuthHandlerInputTest.postingNullEntryIsRejected", ".txt");

        AssertExtensions.assertThrows(NullPointerException.class, () -> objectUnderTest.postEntry(null));
        AssertExtensions.assertThrows(NullPointerException.class, () -> objectUnderTest.postEntries(null));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> objectUnderTest.postEntries(new ArrayList<>()));
    }
}
