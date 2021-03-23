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
package io.pravega.cli.admin.password;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PasswordFileEntryParserTest {

    @Test
    public void parseThrowsExceptionForInvalidInput() {
        AssertExtensions.assertThrows(NullPointerException.class, () -> PasswordFileEntryParser.parse(null));
        AssertExtensions.assertThrows(NullPointerException.class, () -> PasswordFileEntryParser.parse(null, true));
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> PasswordFileEntryParser.parse("username:hashedPassword"));
    }

    @Test
    public void parsesExpectedInput() {
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword:prn::*,READ_UPDATE").length == 3);
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword", false).length == 2);
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword:prn::/,READ_UPDATE;prn::/scope,READ_UPDATE").length == 3);
    }
}
