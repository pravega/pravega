/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.testcommon.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the StringUtils class.
 */
public class StringUtilsTests {
    /**
     * Tests the parsePairs() method.
     */
    @Test
    public void testParsePairs() {
        // Null and Empty String.
        AssertExtensions.assertThrows(
                "parsePairs accepted null input string.",
                () -> StringUtils.parsePairs(null, ",", "=", (k, v) -> Assert.fail("Unexpected callback invocation (null).")),
                ex -> ex instanceof NullPointerException);

        StringUtils.parsePairs("", ",", "=", (k, v) -> Assert.fail("Unexpected callback invocation (empty)."));

        // pairDelimiter == keyValueDelimiter
        AssertExtensions.assertThrows(
                "parsePairs accepted same value for pair and value delimiters.",
                () -> StringUtils.parsePairs("a=b", ",", ",", (k, v) -> Assert.fail("Unexpected callback invocation (delimiters).")),
                ex -> ex instanceof IllegalArgumentException);

        // Set of values with:
        // 1. non-empty keys and values
        // 2. empty key + non-empty value
        // 3. non-empty key + empty value
        String s1 = "k1=v1,=v2,k3=v3,k4=,k5=v5";
        List<String> expected = Arrays.asList("k1|v1", "|v2", "k3|v3", "k4|", "k5|v5");
        List<String> actual = new ArrayList<>();
        StringUtils.parsePairs(s1, ",", "=", (k, v) -> addToList(k, v, actual));
        AssertExtensions.assertListEquals("Unexpected values for fully valid input.", expected, actual, String::equals);

        // Valid values + a "pair" with No keyValueDelimiter.
        String s2 = "k1=v1,=v2,k3=v3,k4=,k5,k6=v6";
        expected = Arrays.asList("k1|v1", "|v2", "k3|v3", "k4|");
        actual.clear();
        AssertExtensions.assertThrows(
                "parsePairs accepted input without a keyValueDelimiter.",
                () -> StringUtils.parsePairs(s2, ",", "=", (k, v) -> addToList(k, v, actual)),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertListEquals("Unexpected values for partially valid input.", expected, actual, String::equals);
    }

    /**
     * Tests the parse() method.
     */
    @Test
    public void testParse() {
        // No need to test the functionality of parsePairs here; just focus on parse.

        // Invalid inputs.
        AssertExtensions.assertThrows(
                "parse accepted invalid Int32 input.",
                () -> StringUtils.parse("a=1b", ",", "=",
                        new StringUtils.Int32Extractor("a", i -> Assert.fail("Unexpected extractor invocation for invalid input (Int32)."))),
                ex -> ex instanceof NumberFormatException);

        AssertExtensions.assertThrows(
                "parse accepted invalid Int64 input.",
                () -> StringUtils.parse("a=1b", ",", "=",
                        new StringUtils.Int64Extractor("a", i -> Assert.fail("Unexpected extractor invocation for invalid input (Int64)."))),
                ex -> ex instanceof NumberFormatException);

        AssertExtensions.assertThrows(
                "parse accepted input with missing extractor reference.",
                () -> StringUtils.parse("a=1", ",", "=",
                        new StringUtils.StringExtractor("b", i -> Assert.fail("Unexpected extractor invocation for invalid input (missing extractor)."))),
                ex -> ex instanceof IllegalArgumentException);

        String s1 = "int=1,long=1234567890123456789,string=myString,bool1=tRuE,bool2=yEs,bool3=1,bool4=false,bool5=foo";
        List<String> expected = Arrays.asList("int|1", "long|1234567890123456789", "string|myString",
                "bool1|true", "bool2|true", "bool3|true", "bool4|false", "bool5|false");
        List<String> actual = new ArrayList<>();
        StringUtils.parse(s1,
                ",",
                "=",
                new StringUtils.Int64Extractor("long", l -> addToList("long", l, actual)),
                new StringUtils.Int32Extractor("int", i -> addToList("int", i, actual)),
                new StringUtils.StringExtractor("string", s -> addToList("string", s, actual)),
                new StringUtils.BooleanExtractor("bool2", b -> addToList("bool2", b, actual)),
                new StringUtils.BooleanExtractor("bool4", b -> addToList("bool4", b, actual)),
                new StringUtils.BooleanExtractor("bool1", b -> addToList("bool1", b, actual)),
                new StringUtils.BooleanExtractor("bool5", b -> addToList("bool5", b, actual)),
                new StringUtils.BooleanExtractor("bool3", b -> addToList("bool3", b, actual)));
        AssertExtensions.assertListEquals("Unexpected values for fully valid input.", expected, actual, String::equals);

        String s2 = "int=1,long=1234567890123456789,string=myString,bool1=tRuE,int2=a,bool2=yEs,bool3=1,bool4=false,bool5=foo";
        expected = Arrays.asList("int|1", "long|1234567890123456789", "string|myString", "bool1|true");
        actual.clear();
        AssertExtensions.assertThrows(
                "parse accepted invalid input.",
                () -> StringUtils.parse(s2,
                        ",",
                        "=",
                        new StringUtils.Int64Extractor("long", l -> addToList("long", l, actual)),
                        new StringUtils.Int32Extractor("int", i -> addToList("int", i, actual)),
                        new StringUtils.Int32Extractor("int2", i -> addToList("int2", i, actual)),
                        new StringUtils.StringExtractor("string", s -> addToList("string", s, actual)),
                        new StringUtils.BooleanExtractor("bool2", b -> addToList("bool2", b, actual)),
                        new StringUtils.BooleanExtractor("bool4", b -> addToList("bool4", b, actual)),
                        new StringUtils.BooleanExtractor("bool1", b -> addToList("bool1", b, actual)),
                        new StringUtils.BooleanExtractor("bool5", b -> addToList("bool5", b, actual)),
                        new StringUtils.BooleanExtractor("bool3", b -> addToList("bool3", b, actual))),
                ex -> ex instanceof NumberFormatException);
        AssertExtensions.assertListEquals("Unexpected values for partially valid input.", expected, actual, String::equals);
    }

    private void addToList(String key, Object value, List<String> target) {
        target.add(String.format("%s|%s", key, value));
    }
}
