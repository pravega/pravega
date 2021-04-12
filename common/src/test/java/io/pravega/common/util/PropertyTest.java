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

public class PropertyTest {

    @Test
    public void testDefaultValue() {
        Property<String> property = Property.named("a");
        assertEquals(false, property.hasDefaultValue());
        assertEquals(null, property.getDefaultValue());
        assertEquals(false, property.hasLegacyName());
        assertEquals(null, property.getLegacyName());
        property = Property.named("a", "default");
        assertEquals(true, property.hasDefaultValue());
        assertEquals("default", property.getDefaultValue());
        assertEquals(false, property.hasLegacyName());
        assertEquals(null, property.getLegacyName());
        property = Property.named("a", "default", "legacy");
        assertEquals(true, property.hasDefaultValue());
        assertEquals("default", property.getDefaultValue());
        assertEquals(true, property.hasLegacyName());
        assertEquals("legacy", property.getLegacyName());
    }
    
    @Test
    public void testString() {
        Property<String> property = Property.named("a", "default");
        assertEquals(false, property.hasLegacyName());
        assertEquals("foo.a", property.getFullName("foo"));
        assertEquals("a", property.getName());
        assertEquals("a", property.toString());
        assertEquals(property, Property.named("a", "default"));
    }
    
}
