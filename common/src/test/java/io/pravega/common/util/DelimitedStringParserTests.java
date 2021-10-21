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

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the DelimitedStringParser class.
 */
public class DelimitedStringParserTests {
    /**
     * Tests the parsePairs() method.
     */
    @Test
    public void testParse() {
        // Null and Empty String.
        AssertExtensions.assertThrows(
                "parsePairs accepted null input string.",
                () -> defaultParser().extractString("a", v -> Assert.fail("Unexpected callback invocation (null)."))
                                     .parse(null),
                ex -> ex instanceof NullPointerException);

        defaultParser().extractString("a", v -> Assert.fail("Unexpected callback invocation (empty)."))
                       .parse("");

        // pairDelimiter == keyValueDelimiter
        AssertExtensions.assertThrows(
                "parsePairs accepted same value for pair and value delimiters.",
                () -> DelimitedStringParser.parser(",", ",")
                                           .extractString("a", v -> Assert.fail("Unexpected callback invocation (delimiters)."))
                                           .parse("a=b"),
                ex -> ex instanceof IllegalArgumentException);

        // Invalid inputs (type-specific).
        AssertExtensions.assertThrows(
                "parse accepted invalid Integer input.",
                () -> defaultParser().extractInteger("a", i -> Assert.fail("Unexpected extractor invocation for invalid input (Integer)."))
                                     .parse("a=1b"),
                ex -> ex instanceof NumberFormatException);

        AssertExtensions.assertThrows(
                "parse accepted invalid Long input.",
                () -> defaultParser().extractLong("a", i -> Assert.fail("Unexpected extractor invocation for invalid input (Long)."))
                                     .parse("a=1b"),
                ex -> ex instanceof NumberFormatException);

        AssertExtensions.assertThrows(
                "parse accepted input with missing extractor reference.",
                () -> defaultParser().extractString("b", i -> Assert.fail("Unexpected extractor invocation for invalid input (missing extractor)."))
                                     .parse("a=1"),
                ex -> ex instanceof IllegalArgumentException);

        String s1 = "int=1,long=1234567890123456789,string=myString,bool1=tRuE,bool2=yEs,bool3=1,bool4=false,bool5=foo";
        List<String> expected = Arrays.asList("int|1", "long|1234567890123456789", "string|myString",
                "bool1|true", "bool2|true", "bool3|true", "bool4|false", "bool5|false");
        List<String> actual = new ArrayList<>();
        defaultParser()
                .extractLong("long", l -> addToList("long", l, actual))
                .extractInteger("int", i -> addToList("int", i, actual))
                .extractString("string", s -> addToList("string", s, actual))
                .extractBoolean("bool2", b -> addToList("bool2", b, actual))
                .extractBoolean("bool4", b -> addToList("bool4", b, actual))
                .extractBoolean("bool1", b -> addToList("bool1", b, actual))
                .extractBoolean("bool5", b -> addToList("bool5", b, actual))
                .extractBoolean("bool3", b -> addToList("bool3", b, actual))
                .parse(s1);
        AssertExtensions.assertListEquals("Unexpected values for fully valid input.", expected, actual, String::equals);

        String s2 = "int=1,long=1234567890123456789,string=myString,bool1=tRuE,int2=a,bool2=yEs,bool3=1,bool4=false,bool5=foo";
        expected = Arrays.asList("int|1", "long|1234567890123456789", "string|myString", "bool1|true");
        actual.clear();
        AssertExtensions.assertThrows(
                "parse accepted invalid input.",
                () -> defaultParser()
                        .extractLong("long", l -> addToList("long", l, actual))
                        .extractInteger("int", i -> addToList("int", i, actual))
                        .extractInteger("int2", i -> addToList("int2", i, actual))
                        .extractString("string", s -> addToList("string", s, actual))
                        .extractBoolean("bool2", b -> addToList("bool2", b, actual))
                        .extractBoolean("bool4", b -> addToList("bool4", b, actual))
                        .extractBoolean("bool1", b -> addToList("bool1", b, actual))
                        .extractBoolean("bool5", b -> addToList("bool5", b, actual))
                        .extractBoolean("bool3", b -> addToList("bool3", b, actual))
                        .parse(s2),
                ex -> ex instanceof NumberFormatException);
        AssertExtensions.assertListEquals("Unexpected values for partially valid input.", expected, actual, String::equals);
    }

    private void addToList(String key, Object value, List<String> target) {
        target.add(String.format("%s|%s", key, value));
    }

    private DelimitedStringParser defaultParser() {
        return DelimitedStringParser.parser(",", "=");
    }
}
