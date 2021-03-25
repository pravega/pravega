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
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnvVarsTest {

    @Test
    public void testReadValue() {
        assertEquals(32, EnvVars.readIntegerFromString("32", "testReadValue", 500));
    }

    @Test
    public void testValueNotThere() {
        assertEquals(500, EnvVars.readIntegerFromString(null, "testValueNotThere", 500));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseFailed() {
        assertEquals(500, EnvVars.readIntegerFromString("sdg", "testParseFailed", 500));
    }

}
